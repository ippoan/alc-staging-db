# alc-staging-db

staging の Worker (vein / leave / dtako / trouble / notify / lineworks) が共用する Postgres。
**Durable Object `StagingDb` の中で PGlite** (@electric-sql/pglite、postgres の wasm 版) を動かし、Worker から `Stub::connect`
で届いた TCP の生バイトを postgres の wire protocol としてそのまま PGlite へ流す。Refs ohishi-exp/rust-leave-worker#1

試作は ippoan/workers-rs-containers-lab の `probe-pglite-db/` (README「PGlite の probe」)。この repo はそれを staging 共用の DB にしたもの。

- **データは揮発する。** DO が起きるたびに空の DB から作り直す (永続化しない)。本番のデータは入れない
- Worker `alc-staging-db-staging` (env.staging) だけを配信する。トップレベル `alc-staging-db` は配信しない
- 外から届く口は無い: Worker の fetch も DO の fetch も常に 404。workers_dev / preview_urls = false、route 無し

## 経路

```
利用側の Worker (env.staging)                    alc-staging-db-staging
  DO binding STAGING_DB ──── Stub::connect ────▶ DO StagingDb.connect(socket)
  tokio-postgres 等 (connect_raw)                   │  直列キュー (1 本ずつ。待ち 20 秒・無通信 30 秒で切る)
                                                    ▼
                                                  PGlite (wasm、DO の isolate の中)
                                                    - 最初の接続で起動: 同梱の datadir を loadDataDir
                                                      → init_local_db → _sqlx_migrations → migration 全量
                                                      → local_app_grants → staging 専用の SQL
                                                    - 接続の頭で SET SESSION AUTHORIZATION alc_api_rt
```

起動の順は `src/steps.js` の `setupSteps`。build 時の `scripts/gen-prebuilt-wasm.mjs` も同じものを node で 1 回通して検査する
(片方だけ変えない)。起動に成功すると 1 行だけ出す:
`{"staging_db":"ready","rev":"<先頭 7 桁>","migrations":<本数>,"ms":<所要>}`。エラーは `{"staging_db":"error","at":…,"kind":…}`
(固定の語と原因の種類だけ。入力値・SQL・tenant_id は出さない)。接続の中のエラーには段 `step`
(`frame` / `start` / `exec` / `role` / `write`) が付く。利用側が Terminate を送らずに切った接続 (tokio-postgres の Client を
drop したまま invocation が終わる形。read が TypeError「client disconnected」で落ちる) はエラーにせず、通常の終了として扱う。

**診断ログ (白黒が付いたら別 PR で削る)**: 「取り込んだページが見えない」の切り分け用 (Refs ohishi-exp/rust-leave-worker#1)。
DO の起動で `{"staging_db":"boot","inst":…}`、ready に `inst` (DO のインスタンスの印) と `gen` (PGlite を作った回数)、
接続ごとに終わるとき 1 行 `{"staging_db":"conn","inst","gen","seq","wait_ms","msgs":{種別:件数},"tags":[CommandComplete の tag],
"errors":[SQLSTATE],"rfq":"I|T|E","end":"terminate|disconnect|idle|eof|error","rolled_back","bind_ts_age_s":[…],"pages",
"tenants","received_age_s":[最小,最大],"bind_uuid_in_pages":[…]}`。
`pages` (leave_pages の全 tenant の総数)・`tenants` (tenant_id の種類数)・`received_age_s` (now() − received_at の秒) は終了処理
(ROLLBACK・RESET) の後に superuser に戻して数え、失敗したら `pages: null`。`bind_ts_age_s` は Bind で届いた時刻の引数の now − 値 (秒)、
`bind_uuid_in_pages` は Bind で届いた uuid の引数ごとに「その値の tenant_id の行が leave_pages に在るか」(値はログに出さない)。

## 接続

- 接続は 1 本ずつ直列に捌く。前の接続が閉じるのを **20 秒**待っても順番が来なければ、新しい接続を閉じる
- 接続中に **30 秒**データが来なければ閉じる (worker::Socket は drop で閉じないので、閉じ忘れで後続が永遠に待つのを防ぐ)
- 接続の頭で `SET SESSION AUTHORIZATION alc_api_rt` (表の所有者ではない実行用ロール。RLS が掛かる)。テナントは
  利用側が `set_config('app.current_tenant_id', …)` で選ぶ (本番の backend と同じ)
- 終了時は開いた transaction を ROLLBACK して `RESET ALL; RESET SESSION AUTHORIZATION`
- **DO は各接続の頭で `SET SESSION AUTHORIZATION alc_api_rt` をし直すので、前の接続の権限は次の接続に残らない**
  (実測: 次の接続の current_user = alc_api_rt、未 commit の BEGIN は ROLLBACK 済み)。
  postgres の session は DO の中で 1 本なので、`RESET` で消えない session の状態 (定義済みの GUC 等) は次の接続に持ち越される

## staging 専用の SQL (会社の行を自動で作る)

staging の利用者の tenant_id は揮発する (DB が起きるたびに空) ので固定値を持てない。migration の後に、この repo の起動 SQL として
(`src/steps.js` の `STAGING_SQL`。alc-migrations には入れない):

- 関数 `alc_api.staging_ensure_tenant()` (trigger、`SECURITY DEFINER`、`SET search_path = alc_api`、所有者は起動を流す superuser):
  `NEW.tenant_id` が tenants に無ければ `(id, name) = (NEW.tenant_id, 'staging')` の行を作る
- `alc_api.tenants(id)` を指す外部キーを持つ alc_api の表すべてに
  `BEFORE INSERT OR UPDATE OF tenant_id … FOR EACH ROW` で張る (rev 57a2775 で 68 表)。
  tenant_id 以外の列の外部キーが見つかったら起動を失敗させる

⇒ 利用側は tenants に無い tenant_id のまま leave_settings・users 等へ書いてよい (外部キー違反にならない)。

## connect 口の認可

**認可は無い。** 接続は superuser の session を alc_api_rt に切り替えたものなので、接続側は
`SET SESSION AUTHORIZATION postgres` (または `SET ROLE postgres`) で superuser に上がれる
(PGlite では `RESET SESSION AUTHORIZATION` / `SET SESSION AUTHORIZATION DEFAULT` は効かず、ロールは変わらない)。
これを次の 3 つの前提で受け入れている:

1. **本番のデータを入れない** — 揮発で、本番の DB への経路も無い
2. **bind できるのは同じ account の Worker だけ** — 外から届く口は無い (fetch は 404、workers_dev / preview_urls = false、
   route 無し。`scripts/check-exposure.sh --private` が CI で検査する)
3. **利用側は staging の env からだけ bind する** — name / class_name / script_name の完全一致で (下)。
   各利用側の check-exposure で縛る

## 利用側の bind の書き方

利用側の `wrangler.toml` の **env.staging にだけ**書く (本番のトップレベルには書かない):

```toml
[[env.staging.durable_objects.bindings]]
name = "STAGING_DB"
class_name = "StagingDb"
script_name = "alc-staging-db-staging"
```

## migration の版の上げ方

`ALC_MIGRATIONS_REV` の 1 行 (ippoan/alc-migrations の commit SHA) だけを書き換える。`wrangler.toml` の `[build]` が
`scripts/fetch-migrations.sh` でその版を取り (写しは commit しない)、`scripts/gen-prebuilt-wasm.mjs` が node で
起動 → migration 全量 → staging 専用の SQL → alc_api_rt での書き・読み (RLS) を 1 回通す。CI の dry-run が通れば、merge で
staging へ配信される。今の版は 57a2775 (migration 172 まで。001〜172 のうち 133 は欠番で 171 本)。

## 128 MB に収めるために

DO の isolate の上限は 128 MB。そのままの PGlite では超えてリセットされる (`exceededMemory`)。lab で効いたのは次の 4 つ
(詳細と実測は lab の README「128 MB に収めるために」):

1. **wasm の線形メモリの宣言**: パッケージの pglite.wasm は `env.memory` を最小 2048 pages (= 128 MiB) で import する。
   build 時に宣言の最小を 512 pages (32 MiB) に書き換えた版を同梱し、`initialMemory` を 32 MiB にする (`PGLITE_INITIAL_MIB`)
2. **initdb を DO で走らせない**: build 時に node で空の datadir を作り (`dumpDataDir("gzip")`)、DO は `loadDataDir` で読むだけ。
   dump は PGlite の版専用なので build のたびに作り直す (`@electric-sql/pglite` は exact 版で固定する)
3. **datadir を小さく**: initdb に `--wal-segsize=1 -c shared_buffers=8MB -c max_connections=5`、template0 / template1 を DROP
4. **postgres の設定**: `shared_buffers=8MB`・`work_mem=1MB`・`maintenance_work_mem=4MB`・`temp_buffers=1MB`・`wal_buffers=1MB`・
   `max_connections=5`

Workers は実行時の wasm のコンパイルを許さないので、PGlite が実行時に bytes からコンパイルする wasm (addFunction の小さな module・
拡張の side module) も build 時に捕まえて同梱し、`src/wasm-shim.ts` が差し替える。`nodejs_compat` は付けず、node の組み込みは
`[alias]` で空のモジュールへ、`import.meta.url` 等は `[define]` で仮の値にする (`wrangler.toml` のコメント)。

## 手元での確認

```sh
npm install --no-audit --no-fund
npm run typecheck
bash scripts/check-exposure.sh --private wrangler.toml
bash scripts/check-exposure-test.sh --private wrangler.toml   # 陰性対照
npx wrangler deploy --dry-run --env staging                   # [build] で migration 全量と検査が走る
```

`wrangler dev` で DO に繋ぐには、DO binding (上の書き方) を持つ Worker の wrangler.toml を並べて
`npx wrangler dev -c <利用側>/wrangler.toml -c wrangler.toml --env staging` にする。

配信は merge 後に `.github/workflows/deploy.yml` が env.staging へ行う (手元から配信しない)。
