"""SQLite 持久化：收藏夹、书签、阅读历史"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "techpulse_user.db")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _close(conn):
    try:
        conn.close()
    except Exception:
        pass


def init_db():
    """初始化表结构（幂等）"""
    conn = get_conn()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS collections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                icon TEXT DEFAULT '📁',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS bookmarks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                collection_id INTEGER NOT NULL,
                article_title TEXT NOT NULL,
                article_url TEXT DEFAULT '',
                tech_category TEXT DEFAULT '',
                ai_summary TEXT DEFAULT '',
                ai_insight TEXT DEFAULT '',
                score REAL DEFAULT 0,
                added_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (collection_id) REFERENCES collections(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS reading_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_title TEXT NOT NULL,
                article_url TEXT DEFAULT '',
                tech_category TEXT DEFAULT '',
                read_at TEXT DEFAULT (datetime('now'))
            );

            INSERT OR IGNORE INTO collections (name, icon) VALUES ('全部收藏', '📂');
            INSERT OR IGNORE INTO collections (name, icon) VALUES ('AI/ML', '🤖');
            INSERT OR IGNORE INTO collections (name, icon) VALUES ('Programming', '💻');
            INSERT OR IGNORE INTO collections (name, icon) VALUES ('CloudNative', '☁️');
            INSERT OR IGNORE INTO collections (name, icon) VALUES ('Security', '🔒');
            INSERT OR IGNORE INTO collections (name, icon) VALUES ('Hardware', '🔧');
            INSERT OR IGNORE INTO collections (name, icon) VALUES ('DataEngineering', '📊');
            INSERT OR IGNORE INTO collections (name, icon) VALUES ('Others', '📂');
        """)
        conn.commit()
    finally:
        _close(conn)


def resolve_collection_id(tech_category: str) -> int:
    """根据技术分类查找匹配的收藏夹 id，未匹配返回 1（全部收藏）"""
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT id FROM collections WHERE name = ? LIMIT 1",
            (tech_category,)
        ).fetchone()
        return row[0] if row else 1
    finally:
        _close(conn)


def list_collections():
    conn = get_conn()
    try:
        rows = conn.execute("SELECT * FROM collections ORDER BY id").fetchall()
        return [dict(r) for r in rows]
    finally:
        _close(conn)


def create_collection(name: str, icon: str = "📁"):
    conn = get_conn()
    try:
        conn.execute("INSERT INTO collections (name, icon) VALUES (?, ?)", (name, icon))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        _close(conn)


def delete_collection(collection_id: int):
    conn = get_conn()
    try:
        conn.execute("DELETE FROM collections WHERE id = ?", (collection_id,))
        conn.commit()
    finally:
        _close(conn)


def add_bookmark(collection_id: int, article: dict):
    conn = get_conn()
    try:
        conn.execute("""
            INSERT INTO bookmarks (collection_id, article_title, article_url, tech_category, ai_summary, ai_insight, score)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            collection_id,
            article.get("title", ""),
            article.get("url", ""),
            article.get("tech_category", ""),
            article.get("ai_summary", ""),
            article.get("ai_insight", ""),
            float(article.get("score", 0) or 0),
        ))
        conn.commit()
    finally:
        _close(conn)


def remove_bookmark(bookmark_id: int):
    conn = get_conn()
    try:
        conn.execute("DELETE FROM bookmarks WHERE id = ?", (bookmark_id,))
        conn.commit()
    finally:
        _close(conn)


def remove_bookmark_by_title(article_title: str):
    """按标题删除书签（用于时间线页面取消收藏）"""
    conn = get_conn()
    try:
        conn.execute("DELETE FROM bookmarks WHERE article_title = ?", (article_title,))
        conn.commit()
    finally:
        _close(conn)


def list_bookmarks(collection_id: int = None):
    conn = get_conn()
    try:
        if collection_id:
            rows = conn.execute(
                "SELECT b.*, c.name as collection_name FROM bookmarks b JOIN collections c ON b.collection_id = c.id WHERE b.collection_id = ? ORDER BY b.added_at DESC",
                (collection_id,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT b.*, c.name as collection_name FROM bookmarks b JOIN collections c ON b.collection_id = c.id ORDER BY b.added_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        _close(conn)


def is_bookmarked(article_title: str) -> bool:
    conn = get_conn()
    try:
        row = conn.execute("SELECT 1 FROM bookmarks WHERE article_title = ? LIMIT 1", (article_title,)).fetchone()
        return row is not None
    finally:
        _close(conn)


def add_to_history(article: dict):
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO reading_history (article_title, article_url, tech_category) VALUES (?, ?, ?)",
            (article.get("title", ""), article.get("url", ""), article.get("tech_category", ""))
        )
        conn.commit()
    finally:
        _close(conn)


def list_history(limit: int = 50):
    conn = get_conn()
    try:
        rows = conn.execute("SELECT * FROM reading_history ORDER BY read_at DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        _close(conn)
