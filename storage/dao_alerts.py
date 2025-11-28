import app.config as cfg

def insert_alert(account_id, source_chat_id, source_chat_title, sender_id, sender_name, sender_username, message_text, matched_keyword, delivered_status, delivered_error=None):
    conn = cfg.pool.connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO alerts (account_id, source_chat_id, source_chat_title, sender_id, sender_name, sender_username, message_text, matched_keyword, delivered_status, delivered_error) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (account_id, source_chat_id, source_chat_title, sender_id, sender_name, sender_username, message_text, matched_keyword, delivered_status, delivered_error)
        )
        conn.commit()
        return cur.lastrowid
    finally:
        cur.close()
        conn.close()
