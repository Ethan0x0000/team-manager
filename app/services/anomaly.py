"""
异常检测服务
用于检测无兑换记录的异常成员并执行清理
"""

import logging
from datetime import datetime
from typing import Any, AsyncGenerator, Dict, List, Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import AnomalyRecord, RedemptionRecord, Team
from app.utils.time_utils import get_now

logger = logging.getLogger(__name__)


class AnomalyService:
    """异常检测服务类"""

    def __init__(self):
        """初始化异常检测服务"""
        pass

    @staticmethod
    def _normalize_email(email: Optional[str]) -> Optional[str]:
        """统一邮箱格式，去除首尾空白。"""
        if not email:
            return None
        normalized = str(email).strip()
        return normalized or None

    @staticmethod
    def _normalize_email_for_compare(email: Optional[str]) -> Optional[str]:
        """统一邮箱比较键，避免大小写差异导致误判。"""
        normalized = AnomalyService._normalize_email(email)
        if not normalized:
            return None
        return normalized.lower()

    @staticmethod
    def _to_iso_string(value: Any) -> Optional[str]:
        """将值转换为 ISO 字符串。"""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.isoformat()
        text = str(value).strip()
        return text or None

    @staticmethod
    def _parse_datetime(value: Any) -> Optional[datetime]:
        """解析各种格式时间，统一为 naive datetime。"""
        if value is None:
            return None

        if isinstance(value, datetime):
            return value.replace(tzinfo=None) if value.tzinfo else value

        raw = str(value).strip()
        if not raw:
            return None

        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed
        except Exception:
            return None

    async def detect_anomalies(
        self, db_session: AsyncSession, team_service: Any
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        检测异常成员（已加入但未绑定任何兑换记录），并流式返回进度。

        Args:
            db_session: 数据库会话
            team_service: Team 服务实例（用于获取成员列表）

        Yields:
            NDJSON 事件对象
        """
        try:
            team_stmt = select(Team).where(
                Team.status.in_(["active", "full"]),
                Team.deleted_at.is_(None),
            )
            team_result = await db_session.execute(team_stmt.order_by(Team.id.asc()))
            teams = team_result.scalars().all()

            total_teams = len(teams)
            yield {"type": "start", "total_teams": total_teams}

            members_to_check: List[Dict[str, Any]] = []

            for index, team in enumerate(teams, start=1):
                try:
                    members_result = await team_service.get_team_members(
                        team.id, db_session
                    )
                except Exception as ex:
                    logger.error("检测 Team %s 成员时失败: %s", team.id, ex)
                    yield {
                        "type": "team_error",
                        "current": index,
                        "total": total_teams,
                        "team_id": team.id,
                        "error": str(ex),
                    }
                    continue

                if not members_result.get("success"):
                    yield {
                        "type": "team_error",
                        "current": index,
                        "total": total_teams,
                        "team_id": team.id,
                        "error": members_result.get("error") or "获取成员列表失败",
                    }
                    continue

                members = members_result.get("members") or []
                yield {
                    "type": "team_progress",
                    "current": index,
                    "total": total_teams,
                    "team_id": team.id,
                    "team_email": team.email,
                    "members_found": len(members),
                }

                for member in members:
                    member_email = self._normalize_email(member.get("email"))
                    member_email_key = self._normalize_email_for_compare(member_email)
                    member_role = str(member.get("role") or "").strip().lower()
                    member_status = str(member.get("status") or "").strip().lower()

                    if (
                        not member_email
                        or not member_email_key
                        or member_role == "account-owner"
                        or member_status != "joined"
                    ):
                        continue

                    members_to_check.append(
                        {
                            "email": member_email,
                            "email_key": member_email_key,
                            "team_id": team.id,
                            "team_name": team.team_name,
                            "user_id": member.get("user_id"),
                            "joined_at": self._to_iso_string(member.get("added_at")),
                        }
                    )

            yield {"type": "check_progress", "message": "正在检查邮箱绑定状态..."}

            email_count_map: Dict[str, int] = {}
            unique_email_keys: set[str] = set()
            for item in members_to_check:
                email_key = item.get("email_key")
                if isinstance(email_key, str) and email_key:
                    unique_email_keys.add(email_key)

            if unique_email_keys:
                normalized_record_email = func.lower(func.trim(RedemptionRecord.email))
                count_result = await db_session.execute(
                    select(
                        normalized_record_email.label("normalized_email"),
                        func.count().label("total"),
                    )
                    .where(normalized_record_email.in_(unique_email_keys))
                    .group_by(normalized_record_email)
                )

                for normalized_email, total in count_result.all():
                    if isinstance(normalized_email, str):
                        email_count_map[normalized_email] = int(total or 0)

            anomalies: List[Dict[str, Any]] = []
            for item in members_to_check:
                email = item.get("email")
                email_key = item.get("email_key")
                if (
                    not email
                    or not isinstance(email_key, str)
                    or email_count_map.get(email_key, 0) > 0
                ):
                    continue

                anomalies.append(
                    {
                        "email": email,
                        "team_id": item.get("team_id"),
                        "team_name": item.get("team_name"),
                        "user_id": item.get("user_id"),
                        "joined_at": item.get("joined_at"),
                    }
                )

            yield {
                "type": "finish",
                "total_teams": total_teams,
                "total_members_checked": len(members_to_check),
                "anomalies": anomalies,
            }
        except Exception:
            logger.exception("检测异常成员失败")
            yield {
                "type": "finish",
                "total_teams": 0,
                "total_members_checked": 0,
                "anomalies": [],
                "error": "检测异常成员失败，请稍后重试",
            }

    async def clean_anomalies(
        self,
        items: List[Dict[str, Any]],
        db_session: AsyncSession,
        team_service: Any,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        清理异常成员，并记录到 anomaly_records。

        Args:
            items: 待清理条目
            db_session: 数据库会话
            team_service: Team 服务实例（用于删除成员）

        Yields:
            NDJSON 事件对象
        """
        total = len(items or [])
        success_count = 0
        failed_count = 0

        yield {"type": "start", "total": total}

        try:
            for index, item in enumerate(items or [], start=1):
                email = self._normalize_email(item.get("email"))
                user_id = str(item.get("user_id") or "").strip()
                team_name = str(item.get("team_name") or "").strip() or None
                raw_team_id = item.get("team_id")

                if raw_team_id is None:
                    failed_count += 1
                    yield {
                        "type": "progress",
                        "current": index,
                        "total": total,
                        "email": email,
                        "team_id": raw_team_id,
                        "success": False,
                        "error": "team_id 无效",
                    }
                    continue

                try:
                    team_id = int(str(raw_team_id))
                except (ValueError, TypeError):
                    failed_count += 1
                    yield {
                        "type": "progress",
                        "current": index,
                        "total": total,
                        "email": email,
                        "team_id": raw_team_id,
                        "success": False,
                        "error": "team_id 无效",
                    }
                    continue

                if not email:
                    failed_count += 1
                    yield {
                        "type": "progress",
                        "current": index,
                        "total": total,
                        "email": email,
                        "team_id": team_id,
                        "success": False,
                        "error": "email 不能为空",
                    }
                    continue

                if not user_id:
                    failed_count += 1
                    yield {
                        "type": "progress",
                        "current": index,
                        "total": total,
                        "email": email,
                        "team_id": team_id,
                        "success": False,
                        "error": "user_id 不能为空",
                    }
                    continue

                try:
                    delete_result = await team_service.delete_team_member(
                        team_id, user_id, db_session, email=email
                    )

                    if not delete_result.get("success"):
                        failed_count += 1
                        await db_session.rollback()
                        yield {
                            "type": "progress",
                            "current": index,
                            "total": total,
                            "email": email,
                            "team_id": team_id,
                            "success": False,
                            "error": delete_result.get("error") or "删除成员失败",
                        }
                        continue

                    anomaly_record = AnomalyRecord(
                        email=email,
                        team_id=team_id,
                        team_name=team_name,
                        joined_at=self._parse_datetime(item.get("joined_at")),
                        deleted_at=get_now(),
                        reason="no_redemption_code",
                    )
                    db_session.add(anomaly_record)
                    await db_session.commit()

                    success_count += 1
                    yield {
                        "type": "progress",
                        "current": index,
                        "total": total,
                        "email": email,
                        "team_id": team_id,
                        "success": True,
                    }
                except Exception as ex:
                    failed_count += 1
                    await db_session.rollback()
                    logger.error(
                        "清理异常成员失败 email=%s, team_id=%s: %s", email, team_id, ex
                    )
                    yield {
                        "type": "progress",
                        "current": index,
                        "total": total,
                        "email": email,
                        "team_id": team_id,
                        "success": False,
                        "error": str(ex),
                    }

            yield {
                "type": "finish",
                "total": total,
                "success_count": success_count,
                "failed_count": failed_count,
            }
        except Exception:
            await db_session.rollback()
            logger.exception("批量清理异常成员失败")
            yield {
                "type": "finish",
                "total": total,
                "success_count": success_count,
                "failed_count": failed_count,
                "error": "批量清理异常成员失败，请稍后重试",
            }

    async def get_all_anomaly_records(
        self,
        db_session: AsyncSession,
        email: Optional[str] = None,
        team_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        获取所有异常清理记录（支持筛选）。

        Args:
            db_session: 数据库会话
            email: 邮箱模糊筛选
            team_id: Team ID 精确筛选

        Returns:
            包含 records 与 total 的字典
        """
        try:
            stmt = select(AnomalyRecord)

            if email:
                stmt = stmt.where(AnomalyRecord.email.ilike(f"%{email}%"))
            if team_id is not None:
                stmt = stmt.where(AnomalyRecord.team_id == team_id)

            stmt = stmt.order_by(AnomalyRecord.deleted_at.desc())
            result = await db_session.execute(stmt)
            records = result.scalars().all()

            record_list = []
            for record in records:
                joined_at = getattr(record, "joined_at", None)
                deleted_at = getattr(record, "deleted_at", None)
                record_list.append(
                    {
                        "id": record.id,
                        "email": record.email,
                        "team_id": record.team_id,
                        "team_name": record.team_name,
                        "joined_at": joined_at.isoformat()
                        if isinstance(joined_at, datetime)
                        else None,
                        "deleted_at": deleted_at.isoformat()
                        if isinstance(deleted_at, datetime)
                        else None,
                        "reason": record.reason,
                    }
                )

            logger.info("获取异常清理记录成功: 共 %s 条", len(record_list))
            return {"records": record_list, "total": len(record_list)}
        except Exception:
            logger.exception("获取异常清理记录失败")
            return {"records": [], "total": 0}
