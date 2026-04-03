"""
数据库自动迁移模块
在应用启动时自动检测并执行必要的数据库迁移
"""

import logging
import sqlite3
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


def get_db_path():
    """获取数据库文件路径"""
    from app.config import settings

    db_file = settings.database_url.split("///")[-1]
    return Path(db_file)


def column_exists(cursor, table_name, column_name):
    """检查表中是否存在指定列"""
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    return column_name in columns


def table_exists(cursor, table_name):
    """检查表是否存在"""
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
    )
    return cursor.fetchone() is not None


def index_exists(cursor, index_name):
    """检查索引是否存在。"""
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name=?", (index_name,)
    )
    return cursor.fetchone() is not None


def run_auto_migration():
    """
    自动运行数据库迁移
    检测缺失的列并自动添加
    """
    db_path = get_db_path()

    if not db_path.exists():
        logger.info("数据库文件不存在，跳过迁移")
        return

    logger.info("开始检查数据库迁移...")

    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        migrations_applied = []

        # 检查并添加质保相关字段
        if not column_exists(cursor, "redemption_codes", "has_warranty"):
            logger.info("添加 redemption_codes.has_warranty 字段")
            cursor.execute("""
                ALTER TABLE redemption_codes 
                ADD COLUMN has_warranty BOOLEAN DEFAULT 0
            """)
            migrations_applied.append("redemption_codes.has_warranty")

        if not column_exists(cursor, "redemption_codes", "warranty_expires_at"):
            logger.info("添加 redemption_codes.warranty_expires_at 字段")
            cursor.execute("""
                ALTER TABLE redemption_codes 
                ADD COLUMN warranty_expires_at DATETIME
            """)
            migrations_applied.append("redemption_codes.warranty_expires_at")

        if not column_exists(cursor, "redemption_codes", "warranty_days"):
            logger.info("添加 redemption_codes.warranty_days 字段")
            cursor.execute("""
                ALTER TABLE redemption_codes 
                ADD COLUMN warranty_days INTEGER DEFAULT 30
            """)
            migrations_applied.append("redemption_codes.warranty_days")

        if not column_exists(cursor, "redemption_records", "is_warranty_redemption"):
            logger.info("添加 redemption_records.is_warranty_redemption 字段")
            cursor.execute("""
                ALTER TABLE redemption_records 
                ADD COLUMN is_warranty_redemption BOOLEAN DEFAULT 0
            """)
            migrations_applied.append("redemption_records.is_warranty_redemption")

        # 检查并添加 Token 刷新相关字段
        if not column_exists(cursor, "teams", "refresh_token_encrypted"):
            logger.info("添加 teams.refresh_token_encrypted 字段")
            cursor.execute("ALTER TABLE teams ADD COLUMN refresh_token_encrypted TEXT")
            migrations_applied.append("teams.refresh_token_encrypted")

        if not column_exists(cursor, "teams", "id_token_encrypted"):
            logger.info("添加 teams.id_token_encrypted 字段")
            cursor.execute("ALTER TABLE teams ADD COLUMN id_token_encrypted TEXT")
            migrations_applied.append("teams.id_token_encrypted")

        if not column_exists(cursor, "teams", "session_token_encrypted"):
            logger.info("添加 teams.session_token_encrypted 字段")
            cursor.execute("ALTER TABLE teams ADD COLUMN session_token_encrypted TEXT")
            migrations_applied.append("teams.session_token_encrypted")

        if not column_exists(cursor, "teams", "client_id"):
            logger.info("添加 teams.client_id 字段")
            cursor.execute("ALTER TABLE teams ADD COLUMN client_id VARCHAR(100)")
            migrations_applied.append("teams.client_id")

        if not column_exists(cursor, "teams", "error_count"):
            logger.info("添加 teams.error_count 字段")
            cursor.execute("ALTER TABLE teams ADD COLUMN error_count INTEGER DEFAULT 0")
            migrations_applied.append("teams.error_count")

        if not column_exists(cursor, "teams", "account_role"):
            logger.info("添加 teams.account_role 字段")
            cursor.execute("ALTER TABLE teams ADD COLUMN account_role VARCHAR(50)")
            migrations_applied.append("teams.account_role")

        if not column_exists(cursor, "teams", "device_code_auth_enabled"):
            logger.info("添加 teams.device_code_auth_enabled 字段")
            cursor.execute(
                "ALTER TABLE teams ADD COLUMN device_code_auth_enabled BOOLEAN DEFAULT 0"
            )
            migrations_applied.append("teams.device_code_auth_enabled")

        if not column_exists(cursor, "teams", "pool_type"):
            logger.info("添加 teams.pool_type 字段")
            cursor.execute(
                "ALTER TABLE teams ADD COLUMN pool_type VARCHAR(20) DEFAULT 'normal'"
            )
            migrations_applied.append("teams.pool_type")

        if not column_exists(cursor, "redemption_codes", "pool_type"):
            logger.info("添加 redemption_codes.pool_type 字段")
            cursor.execute(
                "ALTER TABLE redemption_codes ADD COLUMN pool_type VARCHAR(20) DEFAULT 'normal'"
            )
            migrations_applied.append("redemption_codes.pool_type")

        if not column_exists(cursor, "redemption_codes", "reusable_by_seat"):
            logger.info("添加 redemption_codes.reusable_by_seat 字段")
            cursor.execute(
                "ALTER TABLE redemption_codes ADD COLUMN reusable_by_seat BOOLEAN DEFAULT 0"
            )
            migrations_applied.append("redemption_codes.reusable_by_seat")

        if not table_exists(cursor, "team_email_mappings"):
            logger.info("创建 team_email_mappings 表")
            cursor.execute("""
                CREATE TABLE team_email_mappings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    team_id INTEGER NOT NULL,
                    email VARCHAR(255) NOT NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'invited',
                    source VARCHAR(20) NOT NULL DEFAULT 'sync',
                    last_seen_at DATETIME,
                    missing_sync_count INTEGER NOT NULL DEFAULT 0,
                    created_at DATETIME,
                    updated_at DATETIME,
                    FOREIGN KEY(team_id) REFERENCES teams(id) ON DELETE CASCADE
                )
            """)
            migrations_applied.append("team_email_mappings")

        if table_exists(cursor, "team_email_mappings") and not column_exists(
            cursor, "team_email_mappings", "missing_sync_count"
        ):
            logger.info("添加 team_email_mappings.missing_sync_count 字段")
            cursor.execute("""
                ALTER TABLE team_email_mappings
                ADD COLUMN missing_sync_count INTEGER NOT NULL DEFAULT 0
            """)
            migrations_applied.append("team_email_mappings.missing_sync_count")

        cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_team_email_unique
            ON team_email_mappings (team_id, email)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_team_email_email
            ON team_email_mappings (email)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_team_email_status
            ON team_email_mappings (team_id, status)
        """)

        # 检查并添加 teams.deleted_at 软删除字段
        if not column_exists(cursor, "teams", "deleted_at"):
            logger.info("添加 teams.deleted_at 软删除字段")
            cursor.execute("ALTER TABLE teams ADD COLUMN deleted_at DATETIME")
            migrations_applied.append("teams.deleted_at")

        # 检查并创建 anomaly_records 表
        if not table_exists(cursor, "anomaly_records"):
            logger.info("创建 anomaly_records 异常检测删除记录表")
            cursor.execute("""
                CREATE TABLE anomaly_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email VARCHAR(255) NOT NULL,
                    team_id INTEGER NOT NULL,
                    team_name VARCHAR(255),
                    joined_at DATETIME,
                    deleted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    reason VARCHAR(255) DEFAULT 'no_redemption_code'
                )
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_anomaly_email
                ON anomaly_records (email)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_anomaly_team_id
                ON anomaly_records (team_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_anomaly_deleted_at
                ON anomaly_records (deleted_at)
            """)
            migrations_applied.append("anomaly_records table")

        # 统一历史兑换记录邮箱格式，减少重复判定歧义
        if table_exists(cursor, "redemption_records"):
            cursor.execute("""
                UPDATE redemption_records
                SET email = lower(trim(email))
                WHERE email IS NOT NULL
            """)

            # 建立唯一索引前先按(code, team_id, email)去重，保留最早记录
            cursor.execute("""
                DELETE FROM redemption_records
                WHERE id NOT IN (
                    SELECT MIN(id)
                    FROM redemption_records
                    GROUP BY code, team_id, email
                )
            """)

            if not index_exists(cursor, "idx_redemption_record_unique_activation"):
                logger.info(
                    "创建 redemption_records 唯一索引 idx_redemption_record_unique_activation"
                )
                cursor.execute("""
                    CREATE UNIQUE INDEX idx_redemption_record_unique_activation
                    ON redemption_records (code, team_id, email)
                """)
                migrations_applied.append("idx_redemption_record_unique_activation")

        if not table_exists(cursor, "redemption_invite_markers"):
            logger.info("创建 redemption_invite_markers 表")
            cursor.execute("""
                CREATE TABLE redemption_invite_markers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code VARCHAR(32) NOT NULL,
                    team_id INTEGER NOT NULL,
                    email VARCHAR(255) NOT NULL,
                    invite_confirmed_at DATETIME,
                    created_at DATETIME,
                    FOREIGN KEY(team_id) REFERENCES teams(id) ON DELETE CASCADE
                )
            """)
            migrations_applied.append("redemption_invite_markers")

        cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_redemption_invite_marker_unique
            ON redemption_invite_markers (code, team_id, email)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_redemption_invite_marker_email
            ON redemption_invite_markers (email)
        """)

        # 提交更改
        conn.commit()

        if migrations_applied:
            logger.info(
                f"数据库迁移完成，应用了 {len(migrations_applied)} 个迁移: {', '.join(migrations_applied)}"
            )
        else:
            logger.info("数据库已是最新版本，无需迁移")

        conn.close()

    except Exception as e:
        logger.error(f"数据库迁移失败: {e}")
        raise


if __name__ == "__main__":
    # 允许直接运行此脚本进行迁移
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    run_auto_migration()
    print("迁移完成")
