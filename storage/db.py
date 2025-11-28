import os
import glob
import app.config as legacy_config

MIGRATIONS_DIR = os.path.join(os.path.dirname(__file__), 'migrations')

def migrate():
    conn = legacy_config.pool.connection()
    cursor = conn.cursor()
    try:
        files = sorted(glob.glob(os.path.join(MIGRATIONS_DIR, '*.sql')))
        for path in files:
            with open(path, 'r', encoding='utf-8') as f:
                sql = f.read()
            for stmt in [s.strip() for s in sql.split(';') if s.strip()]:
                try:
                    cursor.execute(stmt)
                except legacy_config.pymysql.err.OperationalError as e:
                    # ignore idempotent failures (duplicate column/index, already exists, can't drop because it doesn't exist)
                    code = e.args[0] if e.args else None
                    msg = str(e)
                    if code in (1060, 1061, 1091) or 'Duplicate column name' in msg or 'Duplicate key name' in msg or "can't drop" in msg.lower() or 'exists' in msg.lower():
                        continue
                    raise
        conn.commit()
    finally:
        cursor.close()
        conn.close()
