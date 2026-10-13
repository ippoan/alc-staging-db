// 接続ごとの診断の 1 行 (Refs ohishi-exp/rust-leave-worker#1。staging で「取り込んだページが見えない」を切り分けるため)。
// 出すのは固定の語と件数だけ: 受けたメッセージの種別ごとの件数、postgres が返した CommandComplete の tag
// ("INSERT 0 2" / "SELECT 0" / "COMMIT" のような語 + 件数)、エラーの SQLSTATE、最後の ReadyForQuery の状態。
// SQL・パラメータ・行の中身・tenant_id は読まない (DataRow 等は種別も数えない)

/// tags に残す上限 (超えた分は数だけ)
const TAGS_MAX = 20;

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

  constructor(
    private readonly seq: number,
    private readonly waitMs: number,
  ) {}

  /// 受けた 1 通の種別を数える (StartupMessage 等は種別の 1 バイトを持たないので語で)
  received(kind: "ssl" | "cancel" | "startup" | "msg", message: Uint8Array): void {
    const k = kind === "msg" ? String.fromCharCode(message[0]) : kind;
    this.msgs[k] = (this.msgs[k] ?? 0) + 1;
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

  line(inst: string, gen: number, pages: number | null): string {
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
      pages,
    });
  }
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
