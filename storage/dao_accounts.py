import app.config as cfg

def create(phone, nickname, username, session_path, status='active'):
    conn = cfg.pool.connection()
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO accounts (phone, nickname, username, session_path, status) VALUES (%s,%s,%s,%s,%s)",
                    (phone, nickname, username, session_path, status))
        conn.commit()
        return cur.lastrowid
    finally:
        cur.close()
        conn.close()

def update_status(account_id, status):
    conn = cfg.pool.connection()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE accounts SET status=%s WHERE id=%s", (status, account_id))
        conn.commit()
    finally:
        cur.close()
        conn.close()

def list_all():
    conn = cfg.pool.connection()
    cur = conn.cursor(cfg.pymysql.cursors.DictCursor)
    try:
        cur.execute("SELECT * FROM accounts ORDER BY id DESC")
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()

def get(account_id):
    conn = cfg.pool.connection()
    cur = conn.cursor(cfg.pymysql.cursors.DictCursor)
    try:
        cur.execute("SELECT * FROM accounts WHERE id=%s", (account_id,))
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def delete(account_id):
    conn = cfg.pool.connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM accounts WHERE id=%s", (account_id,))
        conn.commit()
    finally:
        cur.close()
        conn.close()
