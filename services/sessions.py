import os
import shutil

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
SESS_DIR = os.path.join(BASE_DIR, 'sessions')

os.makedirs(SESS_DIR, exist_ok=True)


def save_session_file(src_path: str, filename: str) -> str:
    base, _ = os.path.splitext(filename or '')
    if not base:
        base = 'session'
    final_name = base + '.session'
    dst = os.path.join(SESS_DIR, final_name)
    if os.path.exists(dst):
        # overwrite by design; could version later
        os.remove(dst)
    shutil.move(src_path, dst)
    os.chmod(dst, 0o600)
    return dst
