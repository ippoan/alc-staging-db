// 接続ごとの診断の 1 行 (Refs ohishi-exp/rust-leave-worker#1。staging で「取り込んだページが見えない」を切り分けるため)。
// 出すのは固定の語と件数だけ: 受けたメッセージの種別ごとの件数、postgres が返した CommandComplete の tag
// ("INSERT 0 2" / "SELECT 0" / "COMMIT" のような語 + 件数)、エラーの SQLSTATE、最後の ReadyForQuery の状態。
// SQL・行の中身は読まない (DataRow 等は種別も数えない)。Bind の引数は時刻と uuid だけを拾い、時刻は now との差 (秒)、
// uuid は「その値の tenant_id の行が leave_pages に在るか」だけにして、値そのもの・tenant_id はログに出さない

/// tags に残す上限 (超えた分は数だけ)
const TAGS_MAX = 20;
/// Bind の引数から拾う時刻・uuid の上限 (接続ごと)
const BIND_MAX = 10;

const OID_TIMESTAMP = 1114;
const OID_TIMESTAMPTZ = 1184;
const OID_UUID = 2950;
/// postgres の時刻の binary の起点 (2000-01-01T00:00:00Z) のミリ秒
const PG_EPOCH_MS = Date.UTC(2000, 0, 1);
const UUID_TEXT = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const TIME_TEXT = /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}/;

/// 接続の終わりに superuser で数えた leave_pages の様子 (src/index.ts の snapshot)
export interface Snapshot {
  pages: number;
  /// tenant_id の種類数
  tenants: number;
  /// [最も新しい行, 最も古い行] の now() − received_at (秒)。行が無ければ []
  received_age_s: number[];
  /// Bind で届いた uuid ごとに、その値の tenant_id の行が在るか
  bind_uuid_in_pages: boolean[];
}

export class ConnStats {
  private readonly msgs: Record<string, number> = {};
  private readonly tags: string[] = [];
  private tagsMore = 0;
  private readonly errors: string[] = [];
  /// 最後の ReadyForQuery の状態 (I = transaction の外 / T = 中 / E = 失敗した transaction の中)
  private rfq = "";
  /// 終わり方: terminate (Terminate を受けた) / disconnect (利用側が切った) / idle / eof / error
  end = "eof";
  /// 終了時に開いたままの transaction を ROLLBACK したか
  rolledBack = false;
  /// 直前の Parse で宣言された引数の型 (名前なしの文の想定)
  private paramTypes: number[] = [];
  /// Bind の時刻の引数の now − 値 (秒)
  private readonly bindTsAges: number[] = [];
  /// Bind の uuid の引数 (メモリにだけ持ち、ログには出さない。snapshot が leave_pages に在るかだけを見る)
  private readonly uuids = new Set<string>();

  constructor(
    private readonly seq: number,
    private readonly waitMs: number,
  ) {}

  /// 受けた 1 通の種別を数える (StartupMessage 等は種別の 1 バイトを持たないので語で)
  received(kind: "ssl" | "cancel" | "startup" | "msg", message: Uint8Array): void {
    const k = kind === "msg" ? String.fromCharCode(message[0]) : kind;
    this.msgs[k] = (this.msgs[k] ?? 0) + 1;
    try {
      if (k === "P") this.parse(message);
      else if (k === "B") this.bind(message);
    } catch {
      // 診断のための読み取り。壊れた形でも接続は止めない
    }
  }

  bindUuids(): string[] {
    return [...this.uuids];
  }

  /// Parse: 名前 → 文 → 引数の型の数 (int16) → 型の oid (int32 ずつ)
  private parse(m: Uint8Array): void {
    const view = new DataView(m.buffer, m.byteOffset, m.byteLength);
    let i = skipCstring(m, skipCstring(m, 5));
    const n = view.getInt16(i);
    i += 2;
    this.paramTypes = [];
    for (let j = 0; j < n; j++, i += 4) this.paramTypes.push(view.getInt32(i));
  }

  /// Bind: portal → 文 → 書式の数と書式 → 引数の数と (長さ, 値) → 結果の書式
  private bind(m: Uint8Array): void {
    const view = new DataView(m.buffer, m.byteOffset, m.byteLength);
    let i = skipCstring(m, skipCstring(m, 5));
    const nFormats = view.getInt16(i);
    i += 2;
    const formats: number[] = [];
    for (let j = 0; j < nFormats; j++, i += 2) formats.push(view.getInt16(i));
    const n = view.getInt16(i);
    i += 2;
    for (let j = 0; j < n; j++) {
      const len = view.getInt32(i);
      i += 4;
      if (len < 0) continue;
      const value = m.subarray(i, i + len);
      i += len;
      const binary = (nFormats === 1 ? formats[0] : formats[j] ?? 0) === 1;
      const oid = this.paramTypes[j] ?? 0;
      if (binary) {
        if ((oid === OID_TIMESTAMPTZ || oid === OID_TIMESTAMP) && len === 8) {
          this.tsAge(PG_EPOCH_MS + Number(new DataView(value.buffer, value.byteOffset, 8).getBigInt64(0) / 1000n));
        } else if (oid === OID_UUID && len === 16) {
          this.uuid(hexUuid(value));
        }
      } else {
        const text = new TextDecoder().decode(value).trim();
        if (UUID_TEXT.test(text)) this.uuid(text.toLowerCase());
        else if (TIME_TEXT.test(text) && !Number.isNaN(Date.parse(text))) this.tsAge(Date.parse(text));
      }
    }
  }

  private tsAge(ms: number): void {
    if (this.bindTsAges.length < BIND_MAX) this.bindTsAges.push(Math.floor((Date.now() - ms) / 1000));
  }

  private uuid(u: string): void {
    if (this.uuids.size < BIND_MAX) this.uuids.add(u);
  }

  /// PGlite が返したバイト列から CommandComplete・ErrorResponse・ReadyForQuery だけを拾う
  sent(chunks: Uint8Array[]): void {
    const total = chunks.reduce((n, c) => n + c.length, 0);
    const buf = new Uint8Array(total);
    let o = 0;
    for (const c of chunks) {
      buf.set(c, o);
      o += c.length;
    }
    const view = new DataView(buf.buffer);
    for (let i = 0; i + 5 <= buf.length; ) {
      const type = buf[i];
      const end = i + 1 + view.getInt32(i + 1);
      if (end > buf.length || end < i + 5) break;
      const body = buf.subarray(i + 5, end);
      if (type === 0x43) this.tag(cstring(body, 0));
      else if (type === 0x45) this.errors.push(errorCode(body));
      else if (type === 0x5a && body.length > 0) this.rfq = String.fromCharCode(body[0]);
      i = end;
    }
  }

  private tag(t: string): void {
    if (this.tags.length < TAGS_MAX) this.tags.push(t);
    else this.tagsMore += 1;
  }

  line(inst: string, gen: number, snap: Snapshot | null): string {
    return JSON.stringify({
      staging_db: "conn",
      inst,
      gen,
      seq: this.seq,
      wait_ms: this.waitMs,
      msgs: this.msgs,
      tags: this.tags,
      ...(this.tagsMore ? { tags_more: this.tagsMore } : {}),
      errors: this.errors,
      rfq: this.rfq,
      end: this.end,
      rolled_back: this.rolledBack,
      bind_ts_age_s: this.bindTsAges,
      ...(snap ?? { pages: null }),
    });
  }
}

/// C 文字列の終わりの次の位置
function skipCstring(body: Uint8Array, from: number): number {
  let to = from;
  while (to < body.length && body[to] !== 0) to += 1;
  return to + 1;
}

function hexUuid(b: Uint8Array): string {
  const h = [...b].map((x) => x.toString(16).padStart(2, "0")).join("");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

function cstring(body: Uint8Array, from: number): string {
  let to = from;
  while (to < body.length && body[to] !== 0) to += 1;
  return new TextDecoder().decode(body.subarray(from, to));
}

/// ErrorResponse の SQLSTATE (field 'C') だけ。message 等は SQL や値を含みうるので読まない
function errorCode(body: Uint8Array): string {
  for (let i = 0; i < body.length && body[i] !== 0; ) {
    const field = body[i];
    const value = cstring(body, i + 1);
    if (field === 0x43) return value;
    i += 1 + new TextEncoder().encode(value).length + 1;
  }
  return "?";
}
