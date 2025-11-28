import app.config as cfg

def add_source(account_id: int, source: str):
    s = (source or '').strip()
    if not s:
        return
    conn = cfg.pool.connection()
    cur = conn.cursor()
    try:
        # skip duplicate
        cur.execute("SELECT 1 FROM listen_sources WHERE account_id=%s AND source=%s LIMIT 1", (account_id, s))
        if cur.fetchone():
            return
        cur.execute("INSERT INTO listen_sources (account_id, source) VALUES (%s,%s)", (account_id, s))
        conn.commit()
    finally:
        cur.close()
        conn.close()

def bulk_add(account_id: int, sources: list[str]):
    if not sources:
        return
    # deduplicate & clean
    seen = set()
    cleaned = []
    for x in sources:
        t = (x or '').strip()
        if not t or t in seen:
            continue
        seen.add(t)
        cleaned.append(t)
    if not cleaned:
        return
    conn = cfg.pool.connection()
    cur = conn.cursor()
    try:
        # filter new ones
        cur.execute("SELECT source FROM listen_sources WHERE account_id=%s", (account_id,))
        existing = {row[0] for row in cur.fetchall()}
        to_add = [(account_id, s) for s in cleaned if s not in existing]
        if to_add:
            cur.executemany("INSERT INTO listen_sources (account_id, source) VALUES (%s,%s)", to_add)
            conn.commit()
    finally:
        cur.close()
        conn.close()

def delete_source(account_id: int, source: str):
    s = (source or '').strip()
    if not s:
        return
    conn = cfg.pool.connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM listen_sources WHERE account_id=%s AND source=%s", (account_id, s))
        conn.commit()
    finally:
        cur.close()
        conn.close()

def clear_sources(account_id: int):
    conn = cfg.pool.connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM listen_sources WHERE account_id=%s", (account_id,))
        conn.commit()
    finally:
        cur.close()
        conn.close()

def list_sources(account_id: int) -> list[str]:
    conn = cfg.pool.connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT source FROM listen_sources WHERE account_id=%s ORDER BY id ASC", (account_id,))
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()
        conn.close()
