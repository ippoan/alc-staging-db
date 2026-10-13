// DO が起動のたびに流す SQL の順 (alc-vein-worker の container/start.sh と同じ):
//   search_path → init_local_db.sql → 空の _sqlx_migrations → migrations (1 ファイル 1 トランザクション)
//   → local_app_grants.sql → staging 専用の SQL (会社の行を自動で作る trigger)
// DO (src/index.ts) と build 時の scripts/gen-prebuilt-wasm.mjs が同じものを流す (片方だけ変えない)。

/// 本番の実行用ロール (表の所有者ではないので、FORCE の無い表でも RLS が掛かる)。migration 158 が作る
export const RUNTIME_ROLE = "alc_api_rt";

/// pglite.wasm の線形メモリの初期値。パッケージの pglite.wasm は最小 2048 pages (128 MiB) を宣言していて
/// isolate の上限 (128 MB) と同じになるので、build 時に宣言を書き換え (scripts/gen-prebuilt-wasm.mjs)、この大きさで始める。
/// 足りなければ wasm が自分で伸ばす (ローカルの実測で migration 全量の後 約 38 MiB)
export const PGLITE_INITIAL_MIB = 32;

/// build 時に datadir を作る initdb の引数 (scripts/gen-prebuilt-wasm.mjs)。WAL のセグメントを 16 → 1 MiB にし、initdb が
/// 中で動かす postgres にも小さい設定を渡す。DO は出来た datadir を loadDataDir で読むだけで、実行時に initdb を走らせない
/// (initdb を DO で動かすと、起動中に initdb.wasm の 64 MiB と既定の shared_buffers のぶんが重なって isolate の上限を超えた)
export const INITDB_PARAMS = ["--wal-segsize=1", "-c", "shared_buffers=8MB", "-c", "max_connections=5"];

/// postgres の設定。既定の shared_buffers (128MB) だと起動だけで wasm のメモリが 約 190 MiB まで伸びる (ローカルの node の実測)
export const POSTGRESQL_CONF = [
  "shared_buffers=8MB",
  "work_mem=1MB",
  "maintenance_work_mem=4MB",
  "temp_buffers=1MB",
  "wal_buffers=1MB",
  "max_connections=5",
];

/// sqlx ではなく SQL で流すので表だけ作る (migration 160 の migration_status() が参照する。中身は空)
const SQLX_MIGRATIONS =
  "CREATE TABLE IF NOT EXISTS alc_api._sqlx_migrations (version BIGINT PRIMARY KEY, description TEXT NOT NULL, installed_on TIMESTAMPTZ NOT NULL DEFAULT now(), success BOOLEAN NOT NULL, checksum BYTEA NOT NULL, execution_time BIGINT NOT NULL)";

/// staging 専用 (alc-migrations には入れない)。データは揮発し、利用者の tenant_id は固定値を持てないので、
/// alc_api.tenants(id) を指す外部キーを持つ表に「tenants に無い tenant_id なら会社の行を作る」trigger を張る。
/// 関数は SECURITY DEFINER で、所有者はこれを流す superuser (alc_api_rt から呼ばれても tenants に書ける)。
/// tenants を指す外部キーは全部が単一列 tenant_id の想定。それ以外が見つかったら起動を失敗させる
export const STAGING_SQL = `
CREATE FUNCTION alc_api.staging_ensure_tenant() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = alc_api AS $fn$
BEGIN
  IF NEW.tenant_id IS NOT NULL THEN
    INSERT INTO alc_api.tenants (id, name) VALUES (NEW.tenant_id, 'staging') ON CONFLICT (id) DO NOTHING;
  END IF;
  RETURN NEW;
END
$fn$;
REVOKE ALL ON FUNCTION alc_api.staging_ensure_tenant() FROM PUBLIC;

DO $do$
DECLARE
  r record;
BEGIN
  -- 分割表の子に複製された外部キー (conparentid <> 0) は親の trigger が子にも張られるので数えない
  FOR r IN
    SELECT c.conrelid::regclass AS tbl, array_agg(a.attname::text ORDER BY a.attnum) AS cols
      FROM pg_constraint c
      JOIN pg_class t ON t.oid = c.conrelid
      JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey)
     WHERE c.contype = 'f'
       AND c.confrelid = 'alc_api.tenants'::regclass
       AND t.relnamespace = 'alc_api'::regnamespace
       AND c.conparentid = 0
     GROUP BY c.oid, c.conrelid
  LOOP
    IF r.cols <> ARRAY['tenant_id'] THEN
      RAISE EXCEPTION 'staging_ensure_tenant: tenants を指す外部キーの列が tenant_id ではない';
    END IF;
  END LOOP;
  FOR r IN
    SELECT DISTINCT c.conrelid::regclass AS tbl
      FROM pg_constraint c
      JOIN pg_class t ON t.oid = c.conrelid
     WHERE c.contype = 'f'
       AND c.confrelid = 'alc_api.tenants'::regclass
       AND t.relnamespace = 'alc_api'::regnamespace
       AND c.conparentid = 0
  LOOP
    EXECUTE format(
      'CREATE TRIGGER staging_ensure_tenant BEFORE INSERT OR UPDATE OF tenant_id ON %s '
      'FOR EACH ROW EXECUTE FUNCTION alc_api.staging_ensure_tenant()', r.tbl);
  END LOOP;
END
$do$;`;

/**
 * @param {{ init: string; grants: string; migrations: { name: string; sql: string }[] }} bundle
 * @returns {{ name: string; sql: string; migration: boolean }[]}
 */
export function setupSteps(bundle) {
  return [
    // init_local_db.sql の ALTER DATABASE … SET search_path は、既に開いているこの 1 本の接続には効かない
    { name: "search_path", sql: "SET search_path TO alc_api, public;", migration: false },
    { name: "init_local_db.sql", sql: bundle.init, migration: false },
    { name: "_sqlx_migrations", sql: SQLX_MIGRATIONS, migration: false },
    ...bundle.migrations.map((m) => ({ name: m.name, sql: `BEGIN;\n${m.sql}\nCOMMIT;`, migration: true })),
    { name: "local_app_grants.sql", sql: bundle.grants, migration: false },
    { name: "staging", sql: STAGING_SQL, migration: false },
  ];
}
