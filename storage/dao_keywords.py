import app.config as cfg


def set_keywords(account_id, keywords, kind: str = 'listen'):
    conn = cfg.pool.connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM keywords WHERE account_id=%s AND kind=%s", (account_id, kind))
        if keywords:
            # trim + deduplicate while preserving input order
            seen = set()
            cleaned = []
            for k in (keywords or []):
                k2 = (k or '').strip()
                if not k2:
                    continue
                if k2 in seen:
                    continue
                seen.add(k2)
                cleaned.append(k2)
            if cleaned:
                cur.executemany(
                    "INSERT INTO keywords (account_id, kind, keyword) VALUES (%s,%s,%s)",
                    [(account_id, kind, k) for k in cleaned]
                )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def add_keyword(account_id: int, word: str, kind: str = 'listen'):
    if not word:
        return
    conn = cfg.pool.connection()
    cur = conn.cursor()
    try:
        w = (word or '').strip()
        if not w:
            return
        # skip duplicate
        cur.execute("SELECT 1 FROM keywords WHERE account_id=%s AND kind=%s AND keyword=%s LIMIT 1", (account_id, kind, w))
        if cur.fetchone():
            return
        cur.execute("INSERT INTO keywords (account_id, kind, keyword) VALUES (%s,%s,%s)", (account_id, kind, w))
        conn.commit()
    finally:
        cur.close()
        conn.close()


def delete_keyword(account_id: int, word: str, kind: str = 'listen'):
    conn = cfg.pool.connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM keywords WHERE account_id=%s AND kind=%s AND keyword=%s", (account_id, kind, word))
        conn.commit()
    finally:
        cur.close()
        conn.close()


def get_keywords(account_id, kind: str = 'listen'):
    conn = cfg.pool.connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT keyword FROM keywords WHERE account_id=%s AND kind=%s ORDER BY id ASC", (account_id, kind))
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()
        conn.close()
