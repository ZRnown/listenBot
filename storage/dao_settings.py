import app.config as cfg

def set_setting(scope, name, value, account_id=None):
    """设置配置项，先删除再插入以解决 NULL 唯一约束问题"""
    conn = cfg.pool.connection()
    cur = conn.cursor()
    try:
        # 先删除旧值（MySQL 的 UNIQUE KEY 对 NULL 不生效，所以需要手动处理）
        if account_id is None:
            cur.execute("DELETE FROM settings WHERE scope=%s AND account_id IS NULL AND name=%s", (scope, name))
        else:
            cur.execute("DELETE FROM settings WHERE scope=%s AND account_id=%s AND name=%s", (scope, account_id, name))
        # 插入新值
        cur.execute("INSERT INTO settings (scope, account_id, name, value) VALUES (%s,%s,%s,%s)",
                    (scope, account_id, name, value))
        conn.commit()
    finally:
        cur.close()
        conn.close()

def get_setting(scope, name, account_id=None):
    """
    Deprecated helper, kept for backward compatibility.
    New code should use get_setting_value().
    """
    return get_setting_value(scope, name, account_id)

def get_setting_value(scope, name, account_id=None):
    conn = cfg.pool.connection()
    cur = conn.cursor()
    try:
        if account_id is None:
            cur.execute("SELECT value FROM settings WHERE scope=%s AND account_id IS NULL AND name=%s", (scope, name))
        else:
            cur.execute("SELECT value FROM settings WHERE scope=%s AND account_id=%s AND name=%s", (scope, account_id, name))
        row = cur.fetchone()
        return None if not row else row[0]
    finally:
        cur.close()
        conn.close()


def delete_account_settings(account_id: int):
    conn = cfg.pool.connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM settings WHERE account_id=%s", (account_id,))
        conn.commit()
    finally:
        cur.close()
        conn.close()


def cleanup_duplicate_global_settings():
    """清理重复的全局设置（保留最新的）"""
    conn = cfg.pool.connection()
    cur = conn.cursor()
    try:
        # 删除所有 account_id 为 NULL 且 scope='global' 且 name='target_bot' 的记录
        cur.execute("DELETE FROM settings WHERE scope='global' AND account_id IS NULL AND name='target_bot'")
        conn.commit()
    finally:
        cur.close()
        conn.close()
