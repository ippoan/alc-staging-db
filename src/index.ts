// Durable Object `StagingDb`: staging の Worker が共用する DB。DO の中で PGlite (@electric-sql/pglite、postgres の wasm 版) を動かし、
// Worker から `Stub::connect` で届いた TCP の生バイトを postgres の wire protocol としてそのまま PGlite へ流す
// (Refs ohishi-exp/rust-leave-worker#1。試作は ippoan/workers-rs-containers-lab の probe-pglite-db/)。
//
// - 外から届く口は無い: Worker の fetch も DO の fetch も常に 404。到達できるのは同じ account で DO binding を持つ Worker だけ。
//   **認可はしない。** 接続は superuser の session を alc_api_rt に切り替えたものなので、接続側が
//   `SET SESSION AUTHORIZATION postgres` すれば superuser に上がれる。受け入れている前提は README の「connect 口の認可」
// - 永続化しない (揮発)。最初の connect の中で起動し、migration 全量と staging 専用の SQL を流す (グローバルスコープでは起動しない)
// - 接続は 1 本ずつ直列に捌く (後から来た接続は前の接続が閉じるまで待つ)。待ちは WAIT_LIMIT_MS まで、接続中に
//   IDLE_LIMIT_MS データが来なければ閉じる (worker::Socket は drop で閉じないので、閉じ忘れで後続が永遠に待つのを防ぐ)
// - frame の切り方は @electric-sql/pglite-socket の PGLiteSocketHandler と同じ (SSLRequest に 'N'、
//   CancelRequest は捨てる、StartupMessage は丸ごと、それ以外は 1 + 長さで 1 通ずつ execProtocolRawStream)
// - ログは起動の 1 行と、エラーの固定の語 + 原因の種類だけ (入力値・SQL・tenant_id を出さない)
import { DurableObject } from "cloudflare:workers";
import { PGlite } from "@electric-sql/pglite";
// Workers は wasm を実行時にコンパイルできないので、wasm と data は bundle に同梱して渡す
// (package の exports が dist を出していないので相対パスで取る)。pglite.wasm だけは memory の宣言を書き換えた版
// (scripts/gen-prebuilt-wasm.mjs が作る)
import pgliteWasm from "../.prebuilt/pglite.wasm";
// initdb.wasm は実行しない (datadir は同梱)。渡さないと PGlite が裏で import.meta.url から取りに行くので、module だけ渡す
import initdbWasm from "../node_modules/@electric-sql/pglite/dist/initdb.wasm";
import pgliteData from "../node_modules/@electric-sql/pglite/dist/pglite.data";
// initdb を済ませた空の datadir (gzip の tar。scripts/gen-prebuilt-wasm.mjs が作る)。実行時に initdb を走らせない
import dataDir from "../.prebuilt/datadir.tar.gz";
// scripts/fetch-migrations.sh が ALC_MIGRATIONS_REV の版から作る (commit しない)
import bundle from "../.alc-migrations/bundle.json";
import { PGLITE_INITIAL_MIB, POSTGRESQL_CONF, RUNTIME_ROLE, setupSteps } from "./steps.js";
import { installWasmShim } from "./wasm-shim";

const SSL_REQUEST = 80877103;
const CANCEL_REQUEST = 80877102;
const PROTOCOL_V3 = 196608;

/// 前の接続が閉じるのを待つ上限。超えたら新しい接続を閉じる
const WAIT_LIMIT_MS = 20_000;
/// 接続中にデータが来ないまま待つ上限。超えたら閉じて次の接続に譲る
const IDLE_LIMIT_MS = 30_000;

/// エラーは固定の語 + 原因の種類 (例外の型と、postgres のエラーなら SQLSTATE) だけを出す。message は SQL や入力値を含みうるので出さない。
/// `step` は接続の中のどの段で起きたか (固定の語)
function logError(at: string, e: unknown, step?: string): void {
  const kind = e instanceof Error ? e.name : typeof e;
  const code = (e as { code?: unknown } | null)?.code;
  console.log(
    JSON.stringify({
      staging_db: "error",
      at,
      ...(step ? { step } : {}),
      kind,
      ...(typeof code === "string" ? { code } : {}),
    }),
  );
}

/// 起動から migration・staging 専用の SQL までを流す。成功したら 1 行だけ出す
async function start(): Promise<PGlite> {
  const started = Date.now();
  let db: PGlite | undefined;
  try {
    installWasmShim();
    db = await PGlite.create({
      pgliteWasmModule: pgliteWasm,
      initdbWasmModule: initdbWasm,
      fsBundle: new Blob([pgliteData]),
      loadDataDir: new Blob([dataDir]),
      initialMemory: PGLITE_INITIAL_MIB * 1024 * 1024,
      postgresqlconf: POSTGRESQL_CONF,
    });
    let migrations = 0;
    for (const s of setupSteps(bundle)) {
      try {
        await db.exec(s.sql);
      } catch (e) {
        if (db.isInTransaction()) await db.exec("ROLLBACK");
        throw e;
      }
      if (s.migration) migrations += 1;
    }
    console.log(
      JSON.stringify({ staging_db: "ready", rev: bundle.rev.slice(0, 7), migrations, ms: Date.now() - started }),
    );
    return db;
  } catch (e) {
    logError(db ? "setup" : "create", e);
    await db?.close().catch(() => {});
    throw e;
  }
}

/// 受け取ったバイト列の頭から、完結した 1 通を切り出す (足りなければ null)。
/// StartupMessage・SSLRequest・CancelRequest は種別の 1 バイトを持たない (長さ + コード)
function nextMessage(buf: Uint8Array, startup: boolean): { kind: "ssl" | "cancel" | "msg"; len: number } | null {
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  if (startup) {
    if (buf.length < 8) return null;
    const len = view.getInt32(0);
    const code = view.getInt32(4);
    // postgres でない先頭は、残りを待たずに閉じる (直列の接続を塞がない)
    if (code !== SSL_REQUEST && code !== CANCEL_REQUEST && code !== PROTOCOL_V3) {
      throw new TypeError("unsupported startup code");
    }
    if (buf.length < len) return null;
    if (len === 8 && code === SSL_REQUEST) return { kind: "ssl", len };
    if (len === 16 && code === CANCEL_REQUEST) return { kind: "cancel", len };
    return { kind: "msg", len };
  }
  if (buf.length < 5) return null;
  const len = 1 + view.getInt32(1);
  return buf.length < len ? null : { kind: "msg", len };
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  const out = new Uint8Array(a.length + b.length);
  out.set(a);
  out.set(b, a.length);
  return out;
}

const IDLE = Symbol("idle");

/// read を IDLE_LIMIT_MS で打ち切る (打ち切ったら IDLE)
async function readWithin<T>(read: Promise<T>): Promise<T | typeof IDLE> {
  let timer: number | null = null;
  const idle = new Promise<typeof IDLE>((resolve) => {
    timer = setTimeout(() => resolve(IDLE), IDLE_LIMIT_MS);
  });
  try {
    return await Promise.race([read, idle]);
  } finally {
    clearTimeout(timer);
  }
}

export class StagingDb extends DurableObject {
  private db: Promise<PGlite> | null = null;
  /// 接続を 1 本ずつに直列化する鎖
  private queue: Promise<void> = Promise.resolve();

  async fetch(): Promise<Response> {
    return new Response("not found", { status: 404 });
  }

  async connect(socket: Socket): Promise<void> {
    // 待ちの上限を超えたら、この接続を閉じて順番が来ても捌かない。connect もその場で返す
    // (socket.close だけでは、connect が返るまで接続側に閉じたことが届かないことがある)
    let expired = false;
    let timer: number | null = null;
    const timedOut = new Promise<void>((resolve) => {
      timer = setTimeout(() => {
        expired = true;
        console.log(JSON.stringify({ staging_db: "error", at: "wait", kind: "timeout" }));
        socket.close().catch(() => {});
        resolve();
      }, WAIT_LIMIT_MS);
    });
    const turn = this.queue.then(() => {
      clearTimeout(timer);
      return expired ? undefined : this.serve(socket);
    });
    this.queue = turn.catch(() => {});
    return Promise.race([turn, timedOut]);
  }

  /// 最初の接続で起動する。起動に失敗したら次の接続で起動し直す
  private async ready(): Promise<PGlite> {
    this.db ??= start();
    try {
      return await this.db;
    } catch (e) {
      this.db = null;
      throw e;
    }
  }

  private async serve(socket: Socket): Promise<void> {
    const writer = socket.writable.getWriter();
    const reader = socket.readable.getReader();
    // PGlite は、最初の postgres のメッセージ (StartupMessage) が届いてから起動する (postgres でない接続では起動しない)
    let db: PGlite | null = null;
    let buf: Uint8Array<ArrayBufferLike> = new Uint8Array(0);
    let startup = true;
    // エラーのログに出す段 (固定の語)
    let step = "frame";
    try {
      read: for (;;) {
        let r: Awaited<ReturnType<typeof reader.read>> | typeof IDLE;
        try {
          r = await readWithin(reader.read());
        } catch {
          // 利用側が Terminate を送らずに切った (tokio-postgres の Client を drop したまま invocation が終わる形)。
          // read は TypeError「client disconnected」で落ちるが、応答は返し終えているので通常の終了として扱う
          // (エラーに出すと commit の取り消しに見える。実際は取り消さない — 開いた transaction だけ finally で ROLLBACK)
          break;
        }
        if (r === IDLE) {
          console.log(JSON.stringify({ staging_db: "error", at: "connection", kind: "idle" }));
          break;
        }
        if (r.done) break;
        buf = concat(buf, r.value);
        for (;;) {
          step = "frame";
          const next = nextMessage(buf, startup);
          if (!next) break;
          const message = buf.slice(0, next.len);
          buf = buf.slice(next.len);
          if (next.kind === "ssl") {
            step = "write";
            await writer.write(new TextEncoder().encode("N"));
            continue;
          }
          if (next.kind === "cancel") continue;
          // Terminate: PGlite には渡さずにここで閉じる (後片付けは finally)
          if (!startup && message[0] === 0x58) break read;
          step = "start";
          db ??= await this.ready();
          const pg = db;
          const out: Uint8Array[] = [];
          step = "exec";
          await pg.runExclusive(() =>
            pg.execProtocolRawStream(message, { onRawData: (chunk) => out.push(chunk.slice()) }),
          );
          if (startup) {
            startup = false;
            step = "role";
            // 応答 (AuthenticationOk … ReadyForQuery) を返す前に、この接続を実行用ロールにする
            await db.exec(`SET SESSION AUTHORIZATION ${RUNTIME_ROLE}`);
          }
          step = "write";
          for (const chunk of out) await writer.write(chunk);
        }
      }
    } catch (e) {
      logError("connection", e, step);
    } finally {
      // 次の接続へ持ち越さない: 開いたままの transaction と SET を戻す。PGlite では RESET SESSION AUTHORIZATION が
      // 効かない (ロールは変わらない) が、次の接続は頭で必ず SET SESSION AUTHORIZATION alc_api_rt をし直す
      try {
        if (db) {
          if (db.isInTransaction()) await db.exec("ROLLBACK");
          await db.exec("RESET ALL; RESET SESSION AUTHORIZATION;");
        }
      } catch (e) {
        logError("reset", e);
      }
      await socket.close().catch(() => {});
    }
  }
}

export default {
  async fetch(): Promise<Response> {
    return new Response("not found", { status: 404 });
  },
} satisfies ExportedHandler;
