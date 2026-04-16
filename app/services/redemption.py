"""
兑换码管理服务
用于管理兑换码的生成、验证、使用和查询
"""

import logging
import secrets
import string
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from sqlalchemy import select, update, delete, and_, or_, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import (
    RedemptionCode,
    RedemptionInviteMarker,
    RedemptionRecord,
    Team,
    TeamEmailMapping,
)
from app.services.settings import (
    settings_service,
    WARRANTY_EXPIRATION_MODE_REFRESH_ON_REDEEM,
)
from app.utils.time_utils import get_now

logger = logging.getLogger(__name__)


class RedemptionService:
    """兑换码管理服务类"""

    def __init__(self):
        """初始化兑换码管理服务"""
        pass

    def _generate_random_code(self, length: int = 16) -> str:
        """
        生成随机兑换码

        Args:
            length: 兑换码长度

        Returns:
            随机兑换码字符串
        """
        # 使用大写字母和数字,排除容易混淆的字符 (0, O, I, 1)
        alphabet = string.ascii_uppercase + string.digits
        alphabet = (
            alphabet.replace("0", "").replace("O", "").replace("I", "").replace("1", "")
        )

        # 生成随机码
        code = "".join(secrets.choice(alphabet) for _ in range(length))

        # 格式化为 XXXX-XXXX-XXXX-XXXX
        if length == 16:
            code = f"{code[0:4]}-{code[4:8]}-{code[8:12]}-{code[12:16]}"

        return code

    @staticmethod
    def _record_sort_key(record: RedemptionRecord) -> tuple[datetime, int]:
        """按兑换时间和记录 ID 排序，确保重建状态时顺序稳定。"""
        return (record.redeemed_at or datetime.min, record.id or 0)

    @staticmethod
    def _clear_code_usage_state(redemption_code: RedemptionCode) -> None:
        """清空兑换码的使用态字段。"""
        redemption_code.status = "unused"
        redemption_code.used_by_email = None
        redemption_code.used_team_id = None
        redemption_code.used_at = None
        redemption_code.warranty_expires_at = None

    @staticmethod
    def _sync_code_status_fields(redemption_code: RedemptionCode) -> bool:
        """按当前时间同步兑换码状态，避免状态被错误地长期停留在过期态。"""
        now = get_now()
        original_status = redemption_code.status

        if redemption_code.used_at:
            if (
                redemption_code.has_warranty
                and redemption_code.warranty_expires_at
                and redemption_code.warranty_expires_at < now
            ):
                redemption_code.status = "expired"
            else:
                redemption_code.status = "used"
            return redemption_code.status != original_status

        if redemption_code.expires_at:
            redemption_code.status = (
                "expired" if redemption_code.expires_at < now else "unused"
            )
        elif redemption_code.status not in {"unused", "used"}:
            redemption_code.status = "unused"

        return redemption_code.status != original_status

    async def _sync_pool_code_statuses(
        self, db_session: AsyncSession, pool_type: Optional[str] = None
    ) -> bool:
        """批量同步指定兑换池中的兑换码状态。"""
        stmt = select(RedemptionCode)
        if pool_type:
            stmt = stmt.where(RedemptionCode.pool_type == pool_type)

        result = await db_session.execute(stmt)
        all_codes = result.scalars().all()

        status_changed = False
        for code in all_codes:
            status_changed = self._sync_code_status_fields(code) or status_changed

        if status_changed:
            await db_session.commit()

        return status_changed

    async def _get_latest_redemption_record(
        self, code: str, db_session: AsyncSession
    ) -> Optional[RedemptionRecord]:
        """获取某个兑换码最后一次兑换记录。"""
        result = await db_session.execute(
            select(RedemptionRecord)
            .where(RedemptionRecord.code == code)
            .order_by(RedemptionRecord.redeemed_at.desc(), RedemptionRecord.id.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    @staticmethod
    def _get_cleanup_reference_time(
        redemption_code: RedemptionCode,
    ) -> Optional[datetime]:
        """获取用于判断历史脏数据冷却期的参考时间。"""
        if redemption_code.warranty_expires_at:
            return redemption_code.warranty_expires_at
        if redemption_code.expires_at:
            return redemption_code.expires_at
        if redemption_code.used_at and not redemption_code.has_warranty:
            # 长期有效但无质保的兑换码，一旦被使用且关联 Team 不可用，就应按“使用时间”进入无效清理冷却期。
            return redemption_code.used_at
        return None

    @staticmethod
    def _is_expired_for_invalid_scan(redemption_code: RedemptionCode) -> bool:
        """无效扫描专用的过期判定：优先看真实时间字段，其次兼容显式 expired 状态。"""
        now = get_now()
        if (
            redemption_code.warranty_expires_at
            and redemption_code.warranty_expires_at < now
        ):
            return True
        if redemption_code.expires_at and redemption_code.expires_at < now:
            return True
        return str(redemption_code.status or "").strip().lower() == "expired"

    @staticmethod
    def _compose_invalid_code_candidate(
        *,
        redemption_code: RedemptionCode,
        record_count: int,
        cleanup_reference_time: Optional[datetime],
        last_team_id: Optional[int],
        last_team_status: str,
        last_record_email: Optional[str],
        last_record_redeemed_at: Optional[datetime],
        cleanup_action: str,
        cleanup_action_label: str,
        reason: str,
        can_remove_from_team: bool,
        can_delete: bool,
    ) -> Dict[str, Any]:
        team_status_summary = [
            {
                "team_id": last_team_id if last_team_id is not None else "-",
                "status": last_team_status,
            }
        ]

        return {
            "code": redemption_code.code,
            "status": "expired",
            "record_count": record_count,
            "expired_at": cleanup_reference_time.isoformat()
            if cleanup_reference_time
            else None,
            "team_statuses": team_status_summary,
            "last_team_id": last_team_id,
            "last_team_status": last_team_status,
            "last_record_email": last_record_email,
            "last_record_redeemed_at": last_record_redeemed_at.isoformat()
            if last_record_redeemed_at
            else None,
            "cleanup_action": cleanup_action,
            "cleanup_action_label": cleanup_action_label,
            "reason": reason,
            "can_remove_from_team": can_remove_from_team,
            "can_delete": can_delete,
            "classification": (
                "needs_team_removal" if can_remove_from_team else "ready_to_delete"
            ),
        }

    @staticmethod
    async def _is_team_email_still_present(
        team_id: Optional[int],
        email: Optional[str],
        db_session: AsyncSession,
    ) -> Optional[bool]:
        """只读检查目标邮箱当前是否仍存在于本地 Team 邮箱映射中。"""
        from app.services.team import (
            ACTIVE_TEAM_EMAIL_STATUSES,
            TEAM_EMAIL_STATUS_REMOVED,
        )

        normalized_email = str(email or "").strip().lower()
        if team_id is None or not normalized_email:
            return None

        result = await db_session.execute(
            select(TeamEmailMapping.status).where(
                TeamEmailMapping.team_id == team_id,
                TeamEmailMapping.email == normalized_email,
            )
        )
        mapping_status = result.scalar_one_or_none()
        if mapping_status is None:
            return None

        normalized_status = str(mapping_status or "").strip().lower()
        if normalized_status in ACTIVE_TEAM_EMAIL_STATUSES:
            return True
        if normalized_status == TEAM_EMAIL_STATUS_REMOVED:
            return False
        return None

    async def _build_invalid_code_candidate(
        self, redemption_code: RedemptionCode, db_session: AsyncSession
    ) -> Optional[Dict[str, Any]]:
        """按“最后一次兑换记录”两层筛查规则分类过期兑换码。"""
        from app.services.team import NORMAL_TEAM_STATUSES, TeamService

        if not self._is_expired_for_invalid_scan(redemption_code):
            return None

        record_count_result = await db_session.execute(
            select(func.count(RedemptionRecord.id)).where(
                RedemptionRecord.code == redemption_code.code
            )
        )
        record_count = int(record_count_result.scalar() or 0)
        cleanup_reference_time = self._get_cleanup_reference_time(redemption_code)
        if cleanup_reference_time is None:
            cleanup_reference_time = (
                redemption_code.expires_at
                or redemption_code.warranty_expires_at
                or redemption_code.used_at
                or redemption_code.created_at
            )

        latest_record = await self._get_latest_redemption_record(
            redemption_code.code, db_session
        )

        last_team_id: Optional[int] = None
        last_team_status = "deleted"
        last_record_email = redemption_code.used_by_email
        last_record_redeemed_at = redemption_code.used_at
        cleanup_action = "no_redemption_record"
        cleanup_action_label = "无兑换记录，可直接删除"
        reason = "兑换码已过期且无可追溯的兑换记录"
        can_remove_from_team = False
        can_delete = True

        if latest_record is not None:
            last_team_id = latest_record.team_id
            last_record_email = latest_record.email
            last_record_redeemed_at = latest_record.redeemed_at

            team = await db_session.get(Team, latest_record.team_id)
            if team is not None and team.deleted_at is None:
                last_team_status = TeamService.get_effective_team_status(team)
                if last_team_status in NORMAL_TEAM_STATUSES:
                    email_still_present = await self._is_team_email_still_present(
                        latest_record.team_id,
                        last_record_email,
                        db_session,
                    )
                    if email_still_present is False:
                        cleanup_action = "team_email_already_absent"
                        cleanup_action_label = "Team 中已无该邮箱，可直接删除"
                        reason = (
                            "最后一次兑换记录对应的 Team 仍正常，但该邮箱已不在当前成员/邀请列表中，"
                            "可直接删除兑换码"
                        )
                        can_remove_from_team = False
                        can_delete = True
                    else:
                        cleanup_action = "requires_team_removal"
                        cleanup_action_label = "需先移出 Team 邮箱"
                        reason = (
                            "最后一次兑换记录对应的 Team 仍正常，请先确认移出该邮箱，"
                            "再执行删除操作"
                        )
                        can_remove_from_team = True
                        can_delete = False
                else:
                    cleanup_action = "team_already_invalid"
                    cleanup_action_label = "最后一次 Team 已失效，可直接删除"
                    reason = "最后一次兑换记录对应的 Team 已失效，当前可直接删除兑换码"
            else:
                cleanup_action = "team_already_invalid"
                cleanup_action_label = "最后一次 Team 已删除，可直接删除"
                reason = "最后一次兑换记录对应的 Team 已删除，当前可直接删除兑换码"
        elif redemption_code.used_team_id is not None:
            last_team_id = redemption_code.used_team_id

        return self._compose_invalid_code_candidate(
            redemption_code=redemption_code,
            record_count=record_count,
            cleanup_reference_time=cleanup_reference_time,
            last_team_id=last_team_id,
            last_team_status=last_team_status,
            last_record_email=last_record_email,
            last_record_redeemed_at=last_record_redeemed_at,
            cleanup_action=cleanup_action,
            cleanup_action_label=cleanup_action_label,
            reason=reason,
            can_remove_from_team=can_remove_from_team,
            can_delete=can_delete,
        )

    async def _collect_invalid_code_candidates(
        self,
        db_session: AsyncSession,
        pool_type: Optional[str] = "normal",
        code_filter: Optional[set[str]] = None,
    ) -> tuple[List[Dict[str, Any]], int, List[str]]:
        """按条件收集无效兑换码候选。"""
        stmt = select(RedemptionCode)
        if pool_type:
            stmt = stmt.where(RedemptionCode.pool_type == pool_type)
        if code_filter:
            stmt = stmt.where(RedemptionCode.code.in_(sorted(code_filter)))

        result = await db_session.execute(
            stmt.order_by(RedemptionCode.created_at.desc())
        )
        codes = result.scalars().all()

        candidates: List[Dict[str, Any]] = []
        scanned_total = 0
        skipped_codes: List[str] = []

        for code in codes:
            if not self._is_expired_for_invalid_scan(code):
                continue

            scanned_total += 1
            candidate = await self._build_invalid_code_candidate(code, db_session)
            if candidate is None:
                skipped_codes.append(code.code)
                continue

            candidates.append(candidate)

        return candidates, scanned_total, skipped_codes

    async def get_invalid_code_candidates(
        self, db_session: AsyncSession, pool_type: Optional[str] = "normal"
    ) -> Dict[str, Any]:
        """扫描过期兑换码，并按最后一次兑换记录分类展示后续动作。"""
        try:
            (
                candidates,
                scanned_total,
                skipped_codes,
            ) = await self._collect_invalid_code_candidates(db_session, pool_type)
            requires_team_removal_total = sum(
                1 for item in candidates if item.get("can_remove_from_team")
            )
            ready_to_delete_total = sum(
                1 for item in candidates if item.get("can_delete")
            )

            return {
                "success": True,
                "codes": candidates,
                "total": len(candidates),
                "scanned_total": scanned_total,
                "requires_team_removal_total": requires_team_removal_total,
                "ready_to_delete_total": ready_to_delete_total,
                "skipped_total": len(skipped_codes),
                "skipped_codes": skipped_codes,
                "error": None,
            }
        except Exception:
            logger.exception("扫描无效兑换码失败")
            return {
                "success": False,
                "codes": [],
                "total": 0,
                "scanned_total": 0,
                "requires_team_removal_total": 0,
                "ready_to_delete_total": 0,
                "skipped_total": 0,
                "skipped_codes": [],
                "error": "扫描无效兑换码失败，请稍后重试",
            }

    async def remove_invalid_code_team_members(
        self,
        codes: List[str],
        db_session: AsyncSession,
        pool_type: Optional[str] = "normal",
    ) -> Dict[str, Any]:
        """对扫描结果中仍绑定正常 Team 的兑换码执行显式移除。"""
        from app.services.team import team_service

        try:
            if not codes:
                return {"success": False, "error": "请选择需要移出 Team 的兑换码"}

            code_filter = {
                str(code or "").strip() for code in codes if str(code or "").strip()
            }
            candidates, _, _ = await self._collect_invalid_code_candidates(
                db_session,
                pool_type=pool_type,
                code_filter=code_filter,
            )
            candidate_map = {item["code"]: item for item in candidates}

            removable_candidates = [
                candidate_map[code]
                for code in codes
                if candidate_map.get(code, {}).get("can_remove_from_team")
            ]
            blocked_codes = [
                code
                for code in codes
                if code not in candidate_map
                or not candidate_map[code].get("can_remove_from_team")
            ]

            if not removable_candidates:
                return {
                    "success": False,
                    "error": "所选兑换码当前无需移出 Team 邮箱，或已不满足处理条件",
                }

            updated_codes: List[Dict[str, Any]] = []
            failed_items: List[Dict[str, str]] = []

            for candidate in removable_candidates:
                team_id = candidate.get("last_team_id")
                email = candidate.get("last_record_email")
                if team_id is None or not email:
                    failed_items.append(
                        {
                            "code": candidate["code"],
                            "error": "缺少最后一次兑换记录对应的 Team 或邮箱信息",
                        }
                    )
                    continue

                team_result = await team_service.remove_invite_or_member(
                    team_id, email, db_session
                )
                if not team_result.get("success"):
                    failed_items.append(
                        {
                            "code": candidate["code"],
                            "error": str(
                                team_result.get("error")
                                or team_result.get("message")
                                or "移除失败"
                            ),
                        }
                    )
                    continue

                team_message = str(team_result.get("message") or "")
                if "不存在" in team_message:
                    cleanup_action_label = "Team 中已无该邮箱，可继续删除"
                    reason = (
                        "最后一次兑换记录对应的 Team 仍正常，但该邮箱已不在 Team 中，"
                        "现在可删除兑换码"
                    )
                else:
                    cleanup_action_label = "已移出 Team 邮箱，可继续删除"
                    reason = "已按确认移出最后一次兑换记录对应 Team 中的邮箱，现在可删除兑换码"

                updated_codes.append(
                    {
                        **candidate,
                        "cleanup_action": "ready_to_delete_after_team_removal",
                        "cleanup_action_label": cleanup_action_label,
                        "reason": reason,
                        "can_remove_from_team": False,
                        "can_delete": True,
                        "classification": "ready_to_delete",
                    }
                )

            if not updated_codes:
                first_error = (
                    failed_items[0]["error"] if failed_items else "移出 Team 邮箱失败"
                )
                return {
                    "success": False,
                    "error": first_error,
                    "failed_codes": failed_items,
                    "blocked_codes": blocked_codes,
                }

            message = f"已处理 {len(updated_codes)} 个 Team 邮箱，可继续删除对应兑换码"
            if failed_items:
                message += f"，另有 {len(failed_items)} 个移除失败"
            if blocked_codes:
                message += f"，{len(blocked_codes)} 个无需移除或已变更"

            return {
                "success": True,
                "message": message,
                "removed_codes": [item["code"] for item in updated_codes],
                "updated_codes": updated_codes,
                "failed_codes": failed_items,
                "blocked_codes": blocked_codes,
                "error": None,
            }
        except Exception:
            logger.exception("移出无效兑换码对应 Team 邮箱失败")
            return {
                "success": False,
                "error": "移出 Team 邮箱失败，请稍后重试",
            }

    async def cleanup_invalid_codes(
        self,
        codes: List[str],
        db_session: AsyncSession,
        pool_type: Optional[str] = "normal",
    ) -> Dict[str, Any]:
        """批量清理通过无效扫描的兑换码。"""
        try:
            if not codes:
                return {"success": False, "error": "请选择需要清理的兑换码"}

            code_filter = {
                str(code or "").strip() for code in codes if str(code or "").strip()
            }
            candidates, _, _ = await self._collect_invalid_code_candidates(
                db_session,
                pool_type=pool_type,
                code_filter=code_filter,
            )

            candidate_map = {item["code"]: item for item in candidates}
            requested_codes = [
                code for code in codes if candidate_map.get(code, {}).get("can_delete")
            ]
            requires_team_removal_codes = [
                code
                for code in codes
                if candidate_map.get(code, {}).get("can_remove_from_team")
            ]
            rejected_codes = [
                code
                for code in codes
                if code not in candidate_map
                or (
                    code not in requested_codes
                    and code not in requires_team_removal_codes
                )
            ]

            if not requested_codes:
                if requires_team_removal_codes:
                    return {
                        "success": False,
                        "error": "所选兑换码仍绑定正常 Team，请先执行“移出 Team 邮箱”再删除",
                        "requires_team_removal_codes": requires_team_removal_codes,
                    }
                return {
                    "success": False,
                    "error": "所选兑换码不满足无效清理条件，已拒绝删除",
                }

            await db_session.execute(
                delete(RedemptionInviteMarker).where(
                    RedemptionInviteMarker.code.in_(requested_codes)
                )
            )
            await db_session.execute(
                delete(RedemptionRecord).where(
                    RedemptionRecord.code.in_(requested_codes)
                )
            )
            await db_session.execute(
                delete(RedemptionCode).where(RedemptionCode.code.in_(requested_codes))
            )
            await db_session.commit()

            message = f"已清理 {len(requested_codes)} 个无效兑换码"
            if rejected_codes:
                message += f"，另有 {len(rejected_codes)} 个因条件不满足被跳过"

            return {
                "success": True,
                "message": message,
                "deleted_codes": requested_codes,
                "skipped_codes": rejected_codes,
                "error": None,
            }
        except Exception:
            await db_session.rollback()
            logger.exception("批量清理无效兑换码失败")
            return {"success": False, "error": "批量清理无效兑换码失败，请稍后重试"}

    async def ensure_virtual_welfare_shadow_code(
        self, db_session: AsyncSession, welfare_code: str
    ) -> Optional[RedemptionCode]:
        """
        为当前福利通用码维护一条仅用于历史记录外键兼容的影子兑换码。
        该记录不会参与真实校验逻辑，当前码的有效性始终以 settings 中的值为准。
        """
        normalized_code = str(welfare_code or "").strip()
        if not normalized_code:
            return None

        result = await db_session.execute(
            select(RedemptionCode).where(RedemptionCode.code == normalized_code)
        )
        shadow_code = result.scalar_one_or_none()

        if shadow_code:
            shadow_code.pool_type = "welfare"
            shadow_code.reusable_by_seat = True
            shadow_code.status = shadow_code.status or "expired"
            shadow_code.has_warranty = False
            shadow_code.warranty_days = 0
            return shadow_code

        shadow_code = RedemptionCode(
            code=normalized_code,
            status="expired",
            has_warranty=False,
            warranty_days=0,
            pool_type="welfare",
            reusable_by_seat=True,
        )
        db_session.add(shadow_code)
        return shadow_code

    async def get_virtual_welfare_code_usage(
        self, db_session: AsyncSession, welfare_code: Optional[str] = None
    ) -> Dict[str, int | str | None]:
        """获取当前福利通用兑换码的实际使用情况。"""
        if welfare_code is None:
            welfare_code = (
                await settings_service.get_setting(
                    db_session, "welfare_common_code", ""
                )
                or ""
            ).strip()

        used_count = 0
        if welfare_code:
            used_result = await db_session.execute(
                select(func.count(RedemptionRecord.id)).where(
                    RedemptionRecord.code == welfare_code
                )
            )
            used_count = int(used_result.scalar() or 0)

        capacity_result = await db_session.execute(
            select(func.sum(Team.max_members - Team.current_members)).where(
                Team.pool_type == "welfare",
                Team.status == "active",
                Team.current_members < Team.max_members,
                Team.deleted_at.is_(None),
            )
        )
        usable_capacity = int(capacity_result.scalar() or 0)

        return {
            "welfare_code": welfare_code,
            "used_count": used_count,
            "usable_capacity": usable_capacity,
            "remaining_count": usable_capacity,
        }

    async def _rebuild_code_usage_state(
        self,
        db_session: AsyncSession,
        redemption_code: RedemptionCode,
        excluding_record_id: Optional[int] = None,
    ) -> int:
        """根据剩余兑换记录重建兑换码状态。"""
        stmt = select(RedemptionRecord).where(
            RedemptionRecord.code == redemption_code.code
        )
        if excluding_record_id is not None:
            stmt = stmt.where(RedemptionRecord.id != excluding_record_id)

        stmt = stmt.order_by(
            RedemptionRecord.redeemed_at.asc(), RedemptionRecord.id.asc()
        )
        result = await db_session.execute(stmt)
        remaining_records = result.scalars().all()

        if not remaining_records:
            self._clear_code_usage_state(redemption_code)
            return 0

        first_record = remaining_records[0]
        latest_record = max(remaining_records, key=self._record_sort_key)

        redemption_code.status = "used"
        redemption_code.used_by_email = latest_record.email
        redemption_code.used_team_id = latest_record.team_id

        if redemption_code.has_warranty:
            expiration_mode = await settings_service.get_warranty_expiration_mode(
                db_session
            )
            base_record = (
                latest_record
                if expiration_mode == WARRANTY_EXPIRATION_MODE_REFRESH_ON_REDEEM
                else first_record
            )
            base_time = base_record.redeemed_at or get_now()
            redemption_code.used_at = base_time
            days = redemption_code.warranty_days or 30
            redemption_code.warranty_expires_at = base_time + timedelta(days=days)
        else:
            redemption_code.used_at = latest_record.redeemed_at or get_now()
            redemption_code.warranty_expires_at = None

        return len(remaining_records)

    async def _can_withdraw_record_without_remote_cleanup(
        self,
        db_session: AsyncSession,
        record: RedemptionRecord,
        team_result: Dict[str, Any],
    ) -> bool:
        """判断 Team 侧失败时是否允许仅做本地撤回。"""
        message = str(team_result.get("message") or "")
        error = str(team_result.get("error") or "")
        combined_text = f"{message} {error}".lower()

        # 远端已确认目标成员不存在，可直接继续本地清理。
        if "成员已不存在" in message or "用户不存在" in error:
            return True

        team = await db_session.get(Team, record.team_id)
        if team is None:
            logger.warning(
                "撤回记录 %s 时 Team %s 不存在，按孤儿记录执行本地清理",
                record.id,
                record.team_id,
            )
            return True

        unavailable_statuses = {"banned", "deleted", "expired", "error"}
        team_status = (team.status or "").strip().lower()

        if team.deleted_at is not None or team_status in unavailable_statuses:
            logger.warning(
                "撤回记录 %s 时 Team %s 当前状态=%s，允许跳过远端移除，仅执行本地清理",
                record.id,
                record.team_id,
                team_status or "unknown",
            )
            return True

        # 防御式兜底：若错误文本明确说明 Team 账号不可用，也允许本地清理。
        unavailable_error_keywords = [
            "account_deactivated",
            "token_invalidated",
            "账号已封禁",
            "team 已删除",
        ]
        if any(keyword in combined_text for keyword in unavailable_error_keywords) and (
            team_status not in {"active", "full"}
        ):
            logger.warning(
                "撤回记录 %s 时检测到 Team 不可用错误(%s)，允许本地清理",
                record.id,
                combined_text,
            )
            return True

        return False

    async def generate_code_single(
        self,
        db_session: AsyncSession,
        code: Optional[str] = None,
        expires_days: Optional[int] = None,
        has_warranty: bool = False,
        warranty_days: int = 30,
        pool_type: str = "normal",
        reusable_by_seat: bool = False,
    ) -> Dict[str, Any]:
        """
        生成单个兑换码

        Args:
            db_session: 数据库会话
            code: 自定义兑换码 (可选,如果不提供则自动生成)
            expires_days: 有效期天数 (可选,如果不提供则永久有效)
            has_warranty: 是否为质保兑换码 (默认 False)

        Returns:
            结果字典,包含 success, code, message, error
        """
        try:
            # 1. 生成或使用自定义兑换码
            if not code:
                # 生成随机码,确保唯一性
                max_attempts = 10
                for _ in range(max_attempts):
                    code = self._generate_random_code()

                    # 检查是否已存在
                    stmt = select(RedemptionCode).where(RedemptionCode.code == code)
                    result = await db_session.execute(stmt)
                    existing = result.scalar_one_or_none()

                    if not existing:
                        break
                else:
                    return {
                        "success": False,
                        "code": None,
                        "message": None,
                        "error": "生成唯一兑换码失败,请重试",
                    }
            else:
                # 检查自定义兑换码是否已存在
                stmt = select(RedemptionCode).where(RedemptionCode.code == code)
                result = await db_session.execute(stmt)
                existing = result.scalar_one_or_none()

                if existing:
                    return {
                        "success": False,
                        "code": None,
                        "message": None,
                        "error": f"兑换码 {code} 已存在",
                    }

            # 2. 计算过期时间
            expires_at = None
            if expires_days:
                expires_at = get_now() + timedelta(days=expires_days)

            # 3. 创建兑换码记录
            redemption_code = RedemptionCode(
                code=code,
                status="unused",
                expires_at=expires_at,
                has_warranty=has_warranty,
                warranty_days=warranty_days,
                pool_type=pool_type,
                reusable_by_seat=reusable_by_seat,
            )

            db_session.add(redemption_code)
            await db_session.commit()

            logger.info(f"生成兑换码成功: {code}")

            return {
                "success": True,
                "code": code,
                "message": f"兑换码生成成功: {code}",
                "error": None,
            }

        except Exception:
            await db_session.rollback()
            logger.exception("生成兑换码失败")
            return {
                "success": False,
                "code": None,
                "message": None,
                "error": "生成兑换码失败，请稍后重试",
            }

    async def generate_code_batch(
        self,
        db_session: AsyncSession,
        count: int,
        expires_days: Optional[int] = None,
        has_warranty: bool = False,
        warranty_days: int = 30,
        pool_type: str = "normal",
        reusable_by_seat: bool = False,
    ) -> Dict[str, Any]:
        """
        批量生成兑换码

        Args:
            db_session: 数据库会话
            count: 生成数量
            expires_days: 有效期天数 (可选)
            has_warranty: 是否为质保兑换码 (默认 False)

        Returns:
            结果字典,包含 success, codes, total, message, error
        """
        try:
            if count <= 0 or count > 1000:
                return {
                    "success": False,
                    "codes": [],
                    "total": 0,
                    "message": None,
                    "error": "生成数量必须在 1-1000 之间",
                }

            # 计算过期时间
            expires_at = None
            if expires_days:
                expires_at = get_now() + timedelta(days=expires_days)

            # 批量生成兑换码
            codes = []
            for i in range(count):
                # 生成唯一兑换码
                max_attempts = 10
                for _ in range(max_attempts):
                    code = self._generate_random_code()

                    # 检查是否已存在 (包括本次批量生成的)
                    if code not in codes:
                        stmt = select(RedemptionCode).where(RedemptionCode.code == code)
                        result = await db_session.execute(stmt)
                        existing = result.scalar_one_or_none()

                        if not existing:
                            codes.append(code)
                            break
                else:
                    logger.warning(f"生成第 {i + 1} 个兑换码失败")
                    continue

            # 批量插入数据库
            for code in codes:
                redemption_code = RedemptionCode(
                    code=code,
                    status="unused",
                    expires_at=expires_at,
                    has_warranty=has_warranty,
                    warranty_days=warranty_days,
                    pool_type=pool_type,
                    reusable_by_seat=reusable_by_seat,
                )
                db_session.add(redemption_code)

            await db_session.commit()

            logger.info(f"批量生成兑换码成功: {len(codes)} 个")

            return {
                "success": True,
                "codes": codes,
                "total": len(codes),
                "message": f"成功生成 {len(codes)} 个兑换码",
                "error": None,
            }

        except Exception:
            await db_session.rollback()
            logger.exception("批量生成兑换码失败")
            return {
                "success": False,
                "codes": [],
                "total": 0,
                "message": None,
                "error": "批量生成兑换码失败，请稍后重试",
            }

    async def validate_code(
        self, code: str, db_session: AsyncSession
    ) -> Dict[str, Any]:
        """
        验证兑换码

        Args:
            code: 兑换码
            db_session: 数据库会话

        Returns:
            结果字典,包含 success, valid, reason, redemption_code, error
        """
        try:
            # 1. 优先按 settings 判断当前福利通用兑换码。
            # 即使数据库里存在用于兼容历史记录外键的影子码，也不能影响当前福利码的真实有效性。
            welfare_code = (
                await settings_service.get_setting(
                    db_session, "welfare_common_code", ""
                )
                or ""
            ).strip()
            if welfare_code and code == welfare_code:
                welfare_usage = await self.get_virtual_welfare_code_usage(
                    db_session, welfare_code=welfare_code
                )
                used_count = int(welfare_usage["used_count"] or 0)
                effective_limit = int(welfare_usage["remaining_count"] or 0)

                if effective_limit <= 0:
                    return {
                        "success": True,
                        "valid": False,
                        "reason": "兑换码次数已用完，无法进行兑换",
                        "redemption_code": None,
                        "error": None,
                    }

                return {
                    "success": True,
                    "valid": True,
                    "reason": "兑换码有效",
                    "redemption_code": {
                        "id": None,
                        "code": code,
                        "status": "virtual_welfare",
                        "expires_at": None,
                        "created_at": None,
                        "has_warranty": False,
                        "warranty_days": 0,
                        "pool_type": "welfare",
                        "reusable_by_seat": True,
                        "virtual_welfare_code": True,
                        "limit": effective_limit,
                        "used_count": used_count,
                    },
                    "error": None,
                }

            # 2. 查询兑换码
            stmt = select(RedemptionCode).where(RedemptionCode.code == code)
            result = await db_session.execute(stmt)
            redemption_code = result.scalar_one_or_none()

            if not redemption_code:
                # 兼容福利通用兑换码：只存 settings，不写 redemption_codes 表
                if welfare_code and code == welfare_code:
                    welfare_usage = await self.get_virtual_welfare_code_usage(
                        db_session, welfare_code=welfare_code
                    )
                    used_count = int(welfare_usage["used_count"] or 0)
                    effective_limit = int(welfare_usage["remaining_count"] or 0)

                    if effective_limit <= 0:
                        return {
                            "success": True,
                            "valid": False,
                            "reason": "兑换码次数已用完，无法进行兑换",
                            "redemption_code": None,
                            "error": None,
                        }

                    return {
                        "success": True,
                        "valid": True,
                        "reason": "兑换码有效",
                        "redemption_code": {
                            "id": None,
                            "code": code,
                            "status": "virtual_welfare",
                            "expires_at": None,
                            "created_at": None,
                            "has_warranty": False,
                            "warranty_days": 0,
                            "pool_type": "welfare",
                            "reusable_by_seat": True,
                            "virtual_welfare_code": True,
                            "limit": effective_limit,
                            "used_count": used_count,
                        },
                        "error": None,
                    }

                return {
                    "success": True,
                    "valid": False,
                    "reason": "兑换码不存在",
                    "redemption_code": None,
                    "error": None,
                }

            # 兼容清理：历史版本中福利通用码可能被写入 redemption_codes。
            # 现版本福利通用码以 settings 为准，旧的福利可复用码一律视为失效，避免绕过“新码替换旧码”的规则。
            if (
                redemption_code.pool_type == "welfare"
                and redemption_code.reusable_by_seat
            ):
                return {
                    "success": True,
                    "valid": False,
                    "reason": "兑换码已失效，请使用最新福利通用兑换码",
                    "redemption_code": None,
                    "error": None,
                }

            # 2. 检查状态
            if redemption_code.reusable_by_seat:
                allowed_statuses = ["unused", "used", "warranty_active"]
            else:
                allowed_statuses = ["unused", "warranty_active"]
                if redemption_code.has_warranty:
                    allowed_statuses.append("used")

            if redemption_code.status not in allowed_statuses:
                status_text = (
                    "已过期"
                    if redemption_code.status == "expired"
                    else redemption_code.status
                )
                reason = (
                    "兑换码已被使用"
                    if redemption_code.status == "used"
                    else f"兑换码{status_text}"
                )
                return {
                    "success": True,
                    "valid": False,
                    "reason": reason,
                    "redemption_code": None,
                    "error": None,
                }

            # 3. 席位可复用兑换码次数限制校验（按池内总席位）
            if redemption_code.reusable_by_seat:
                total_seats_stmt = select(func.sum(Team.max_members)).where(
                    Team.pool_type == (redemption_code.pool_type or "normal"),
                    Team.deleted_at.is_(None),
                )
                total_seats_result = await db_session.execute(total_seats_stmt)
                total_seats = int(total_seats_result.scalar() or 0)

                used_count_stmt = select(func.count(RedemptionRecord.id)).where(
                    RedemptionRecord.code == code
                )
                used_count_result = await db_session.execute(used_count_stmt)
                used_count = int(used_count_result.scalar() or 0)

                if total_seats <= 0 or used_count >= total_seats:
                    return {
                        "success": True,
                        "valid": False,
                        "reason": "兑换码次数已用完，无法进行兑换",
                        "redemption_code": None,
                        "error": None,
                    }

            status_changed = self._sync_code_status_fields(redemption_code)
            if status_changed:
                await db_session.flush()

            # 4. 检查质保是否已过期（针对已使用的质保码）
            if redemption_code.status == "expired" and redemption_code.used_at:
                redemption_code.status = "expired"
                return {
                    "success": True,
                    "valid": False,
                    "reason": "质保已过期",
                    "redemption_code": None,
                    "error": None,
                }

            # 5. 检查是否过期 (仅针对未使用的兑换码执行首次激活截止时间检查)
            if redemption_code.status == "expired" and not redemption_code.used_at:
                return {
                    "success": True,
                    "valid": False,
                    "reason": "兑换码已过期 (超过首次兑换截止时间)",
                    "redemption_code": None,
                    "error": None,
                }

            # 6. 验证通过
            return {
                "success": True,
                "valid": True,
                "reason": "兑换码有效",
                "redemption_code": {
                    "id": redemption_code.id,
                    "code": redemption_code.code,
                    "status": redemption_code.status,
                    "expires_at": redemption_code.expires_at.isoformat()
                    if redemption_code.expires_at
                    else None,
                    "created_at": redemption_code.created_at.isoformat()
                    if redemption_code.created_at
                    else None,
                    "has_warranty": redemption_code.has_warranty,
                    "warranty_days": redemption_code.warranty_days,
                    "pool_type": redemption_code.pool_type or "normal",
                    "reusable_by_seat": bool(redemption_code.reusable_by_seat),
                },
                "error": None,
            }

        except Exception:
            logger.exception("验证兑换码失败")
            return {
                "success": False,
                "valid": False,
                "reason": None,
                "redemption_code": None,
                "error": "验证兑换码失败，请稍后重试",
            }

    async def use_code(
        self,
        code: str,
        email: str,
        team_id: int,
        account_id: str,
        db_session: AsyncSession,
    ) -> Dict[str, Any]:
        """
        使用兑换码

        Args:
            code: 兑换码
            email: 使用者邮箱
            team_id: Team ID
            account_id: Account ID
            db_session: 数据库会话

        Returns:
            结果字典,包含 success, message, error
        """
        try:
            # 1. 验证兑换码
            validate_result = await self.validate_code(code, db_session)

            if not validate_result["success"]:
                return {
                    "success": False,
                    "message": None,
                    "error": validate_result["error"],
                }

            if not validate_result["valid"]:
                return {
                    "success": False,
                    "message": None,
                    "error": validate_result["reason"],
                }

            # 2. 更新兑换码状态
            stmt = select(RedemptionCode).where(RedemptionCode.code == code)
            result = await db_session.execute(stmt)
            redemption_code = result.scalar_one_or_none()

            redemption_code.status = "used"
            redemption_code.used_by_email = email
            redemption_code.used_team_id = team_id
            redemption_code.used_at = get_now()

            # 3. 创建使用记录
            redemption_record = RedemptionRecord(
                email=email, code=code, team_id=team_id, account_id=account_id
            )

            db_session.add(redemption_record)
            await db_session.commit()

            logger.info(f"使用兑换码成功: {code} -> {email}")

            return {"success": True, "message": "兑换码使用成功", "error": None}

        except Exception:
            await db_session.rollback()
            logger.exception("使用兑换码失败")
            return {
                "success": False,
                "message": None,
                "error": "使用兑换码失败，请稍后重试",
            }

    async def get_all_codes(
        self,
        db_session: AsyncSession,
        page: int = 1,
        per_page: int = 50,
        search: Optional[str] = None,
        status: Optional[str] = None,
        pool_type: Optional[str] = "normal",
    ) -> Dict[str, Any]:
        """
        获取所有兑换码

        Args:
            db_session: 数据库会话
            page: 页码
            per_page: 每页数量
            search: 搜索关键词 (兑换码或邮箱)
            status: 状态筛选

        Returns:
            结果字典,包含 success, codes, total, total_pages, current_page, error
        """
        try:
            await self._sync_pool_code_statuses(db_session, pool_type)

            # 1. 构建基础查询
            count_stmt = select(func.count(RedemptionCode.id))
            stmt = select(RedemptionCode).order_by(RedemptionCode.created_at.desc())

            # 2. 如果提供了筛选条件,添加过滤条件
            filters = []
            if search:
                filters.append(
                    or_(
                        RedemptionCode.code.ilike(f"%{search}%"),
                        RedemptionCode.used_by_email.ilike(f"%{search}%"),
                    )
                )

            if status:
                if status == "used":
                    # "已使用" 在查询中通常指窄义的 used, 但如果要包含质保中, 逻辑如下
                    filters.append(
                        RedemptionCode.status.in_(["used", "warranty_active"])
                    )
                else:
                    filters.append(RedemptionCode.status == status)

            if filters:
                count_stmt = count_stmt.where(and_(*filters))
                stmt = stmt.where(and_(*filters))

            # 3. 获取总数
            count_result = await db_session.execute(count_stmt)
            total = count_result.scalar() or 0

            # 4. 计算分页
            import math

            total_pages = math.ceil(total / per_page) if total > 0 else 1
            if page < 1:
                page = 1
            if page > total_pages and total_pages > 0:
                page = total_pages

            offset = (page - 1) * per_page

            # 5. 查询分页数据
            stmt = stmt.limit(per_page).offset(offset)
            result = await db_session.execute(stmt)
            codes = result.scalars().all()

            record_counts: Dict[str, int] = {}
            if codes:
                code_values = [code.code for code in codes]
                record_count_result = await db_session.execute(
                    select(RedemptionRecord.code, func.count(RedemptionRecord.id))
                    .where(RedemptionRecord.code.in_(code_values))
                    .group_by(RedemptionRecord.code)
                )
                record_counts = {
                    record_code: int(record_total or 0)
                    for record_code, record_total in record_count_result.all()
                }

            # 构建返回数据
            code_list = []
            for code in codes:
                code_list.append(
                    {
                        "id": code.id,
                        "code": code.code,
                        "status": code.status,
                        "created_at": code.created_at.isoformat()
                        if code.created_at
                        else None,
                        "expires_at": code.expires_at.isoformat()
                        if code.expires_at
                        else None,
                        "used_by_email": code.used_by_email,
                        "used_team_id": code.used_team_id,
                        "used_at": code.used_at.isoformat() if code.used_at else None,
                        "has_warranty": code.has_warranty,
                        "warranty_days": code.warranty_days,
                        "warranty_expires_at": code.warranty_expires_at.isoformat()
                        if code.warranty_expires_at
                        else None,
                        "can_delete": record_counts.get(code.code, 0) == 0,
                    }
                )

            logger.info(
                f"获取所有兑换码成功: 第 {page} 页, 共 {len(code_list)} 个 / 总数 {total}"
            )

            return {
                "success": True,
                "codes": code_list,
                "total": total,
                "total_pages": total_pages,
                "current_page": page,
                "error": None,
            }

        except Exception:
            logger.exception("获取所有兑换码失败")
            return {
                "success": False,
                "codes": [],
                "total": 0,
                "error": "获取所有兑换码失败，请稍后重试",
            }

    async def get_unused_count(self, db_session: AsyncSession) -> int:
        """
        获取未使用的兑换码数量
        """
        try:
            stmt = select(func.count(RedemptionCode.id)).where(
                RedemptionCode.status == "unused"
            )
            result = await db_session.execute(stmt)
            return result.scalar() or 0
        except Exception as e:
            logger.error(f"获取未使用兑换码数量失败: {e}")
            return 0

    async def get_code_by_code(
        self, code: str, db_session: AsyncSession
    ) -> Dict[str, Any]:
        """
        根据兑换码查询

        Args:
            code: 兑换码
            db_session: 数据库会话

        Returns:
            结果字典,包含 success, code_info, error
        """
        try:
            stmt = select(RedemptionCode).where(RedemptionCode.code == code)
            result = await db_session.execute(stmt)
            redemption_code = result.scalar_one_or_none()

            if not redemption_code:
                return {
                    "success": False,
                    "code_info": None,
                    "error": f"兑换码 {code} 不存在",
                }

            code_info = {
                "id": redemption_code.id,
                "code": redemption_code.code,
                "status": redemption_code.status,
                "created_at": redemption_code.created_at.isoformat()
                if redemption_code.created_at
                else None,
                "expires_at": redemption_code.expires_at.isoformat()
                if redemption_code.expires_at
                else None,
                "used_by_email": redemption_code.used_by_email,
                "used_team_id": redemption_code.used_team_id,
                "used_at": redemption_code.used_at.isoformat()
                if redemption_code.used_at
                else None,
            }

            return {"success": True, "code_info": code_info, "error": None}

        except Exception:
            logger.exception("查询兑换码失败")
            return {
                "success": False,
                "code_info": None,
                "error": "查询兑换码失败，请稍后重试",
            }

    async def get_unused_codes(self, db_session: AsyncSession) -> Dict[str, Any]:
        """
        获取未使用的兑换码

        Args:
            db_session: 数据库会话

        Returns:
            结果字典,包含 success, codes, total, error
        """
        try:
            stmt = (
                select(RedemptionCode)
                .where(RedemptionCode.status == "unused")
                .order_by(RedemptionCode.created_at.desc())
            )

            result = await db_session.execute(stmt)
            codes = result.scalars().all()

            # 构建返回数据
            code_list = []
            for code in codes:
                code_list.append(
                    {
                        "id": code.id,
                        "code": code.code,
                        "status": code.status,
                        "created_at": code.created_at.isoformat()
                        if code.created_at
                        else None,
                        "expires_at": code.expires_at.isoformat()
                        if code.expires_at
                        else None,
                    }
                )

            return {
                "success": True,
                "codes": code_list,
                "total": len(code_list),
                "error": None,
            }

        except Exception:
            logger.exception("获取未使用兑换码失败")
            return {
                "success": False,
                "codes": [],
                "total": 0,
                "error": "获取未使用兑换码失败，请稍后重试",
            }

    async def get_all_records(
        self,
        db_session: AsyncSession,
        email: Optional[str] = None,
        code: Optional[str] = None,
        team_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        获取所有兑换记录 (支持筛选)

        Args:
            db_session: 数据库会话
            email: 邮箱模糊搜索
            code: 兑换码模糊搜索
            team_id: Team ID 筛选

        Returns:
            结果字典,包含 success, records, total, error
        """
        try:
            stmt = select(RedemptionRecord)

            # 添加筛选条件
            filters = []
            if email:
                filters.append(RedemptionRecord.email.ilike(f"%{email}%"))
            if code:
                filters.append(RedemptionRecord.code.ilike(f"%{code}%"))
            if team_id:
                filters.append(RedemptionRecord.team_id == team_id)

            if filters:
                stmt = stmt.where(and_(*filters))

            stmt = stmt.order_by(RedemptionRecord.redeemed_at.desc())

            result = await db_session.execute(stmt)
            records = result.scalars().all()

            # 构建返回数据
            record_list = []
            for record in records:
                record_list.append(
                    {
                        "id": record.id,
                        "email": record.email,
                        "code": record.code,
                        "team_id": record.team_id,
                        "account_id": record.account_id,
                        "redeemed_at": record.redeemed_at.isoformat()
                        if record.redeemed_at
                        else None,
                    }
                )

            logger.info(f"获取所有兑换记录成功: 共 {len(record_list)} 条")

            return {
                "success": True,
                "records": record_list,
                "total": len(record_list),
                "error": None,
            }

        except Exception:
            logger.exception("获取所有兑换记录失败")
            return {
                "success": False,
                "records": [],
                "total": 0,
                "error": "获取所有兑换记录失败，请稍后重试",
            }

    async def delete_code(self, code: str, db_session: AsyncSession) -> Dict[str, Any]:
        """
        删除兑换码

        Args:
            code: 兑换码
            db_session: 数据库会话

        Returns:
            结果字典,包含 success, message, error
        """
        try:
            # 查询兑换码
            stmt = select(RedemptionCode).where(RedemptionCode.code == code)
            result = await db_session.execute(stmt)
            redemption_code = result.scalar_one_or_none()

            if not redemption_code:
                return {
                    "success": False,
                    "message": None,
                    "error": f"兑换码 {code} 不存在",
                }

            self._sync_code_status_fields(redemption_code)

            # 已产生历史记录的兑换码不能直接删除，否则会破坏使用记录完整性。
            record_count_result = await db_session.execute(
                select(func.count(RedemptionRecord.id)).where(
                    RedemptionRecord.code == code
                )
            )
            record_count = int(record_count_result.scalar() or 0)
            if record_count > 0:
                return {
                    "success": False,
                    "message": None,
                    "error": f"兑换码 {code} 已有 {record_count} 条关联记录，无法直接删除",
                }

            # 删除兑换码
            await db_session.delete(redemption_code)
            await db_session.commit()

            logger.info(f"删除兑换码成功: {code}")

            return {"success": True, "message": f"兑换码 {code} 已删除", "error": None}

        except Exception:
            await db_session.rollback()
            logger.exception("删除兑换码失败")
            return {
                "success": False,
                "message": None,
                "error": "删除兑换码失败，请稍后重试",
            }

    async def update_code(
        self,
        code: str,
        db_session: AsyncSession,
        has_warranty: Optional[bool] = None,
        warranty_days: Optional[int] = None,
    ) -> Dict[str, Any]:
        """更新兑换码信息"""
        return await self.bulk_update_codes(
            [code], db_session, has_warranty, warranty_days
        )

    async def withdraw_record(
        self, record_id: int, db_session: AsyncSession
    ) -> Dict[str, Any]:
        """
        撤回使用记录 (删除记录,恢复兑换码,并在 Team 中移除成员/邀请)

        Args:
            record_id: 记录 ID
            db_session: 数据库会话

        Returns:
            结果字典
        """
        try:
            from app.services.team import team_service

            # 1. 查询记录
            stmt = (
                select(RedemptionRecord)
                .where(RedemptionRecord.id == record_id)
                .options(selectinload(RedemptionRecord.redemption_code))
            )
            result = await db_session.execute(stmt)
            record = result.scalar_one_or_none()

            if not record:
                return {"success": False, "error": f"记录 ID {record_id} 不存在"}

            # 2. 调用 TeamService 移除成员/邀请
            logger.info(f"正在从 Team {record.team_id} 中移除成员 {record.email}")
            team_result = await team_service.remove_invite_or_member(
                record.team_id, record.email, db_session
            )

            if not team_result["success"]:
                # Team 不可操作（封禁/删除/过期/异常）时，允许仅执行本地撤回，避免形成删除死锁。
                if not await self._can_withdraw_record_without_remote_cleanup(
                    db_session,
                    record,
                    team_result,
                ):
                    return {
                        "success": False,
                        "error": f"从 Team 移除成员失败: {team_result.get('error') or team_result.get('message')}",
                    }

            # 3. 根据剩余记录重建兑换码状态
            code = record.redemption_code
            remaining_records_count = 0
            if code:
                remaining_records_count = await self._rebuild_code_usage_state(
                    db_session, code, excluding_record_id=record.id
                )

            # 4. 删除使用记录
            await db_session.delete(record)
            await db_session.commit()

            logger.info(
                f"撤回记录成功: {record_id}, 邮箱: {record.email}, 兑换码: {record.code}"
            )

            if code and remaining_records_count > 0:
                message = f"成功撤回记录，兑换码 {record.code} 已按剩余 {remaining_records_count} 条记录重建状态"
            else:
                message = f"成功撤回记录并恢复兑换码 {record.code}"

            return {"success": True, "message": message}

        except Exception:
            await db_session.rollback()
            logger.exception("撤回记录失败")
            return {"success": False, "error": "撤回失败，请稍后重试"}

    async def bulk_update_codes(
        self,
        codes: List[str],
        db_session: AsyncSession,
        has_warranty: Optional[bool] = None,
        warranty_days: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        批量更新兑换码信息

        Args:
            codes: 兑换码列表
            db_session: 数据库会话
            has_warranty: 是否为质保兑换码 (可选)
            warranty_days: 质保天数 (可选)

        Returns:
            结果字典
        """
        try:
            if not codes:
                return {"success": True, "message": "没有需要更新的兑换码"}

            # 构建更新语句
            values = {}
            if has_warranty is not None:
                values[RedemptionCode.has_warranty] = has_warranty
            if warranty_days is not None:
                values[RedemptionCode.warranty_days] = warranty_days

            if not values:
                return {"success": True, "message": "没有提供更新内容"}

            stmt = (
                update(RedemptionCode)
                .where(RedemptionCode.code.in_(codes))
                .values(values)
            )
            await db_session.execute(stmt)
            await db_session.flush()

            # 对已使用的兑换码重新计算 warranty_expires_at，
            # 否则修改 warranty_days / has_warranty 后过期判定仍按旧值。
            affected_stmt = select(RedemptionCode).where(
                RedemptionCode.code.in_(codes),
                RedemptionCode.used_at.isnot(None),
            )
            affected_result = await db_session.execute(affected_stmt)
            affected_codes = affected_result.scalars().all()

            for code_obj in affected_codes:
                await self._rebuild_code_usage_state(db_session, code_obj)

            await db_session.commit()

            logger.info(f"成功批量更新 {len(codes)} 个兑换码")

            return {
                "success": True,
                "message": f"成功批量更新 {len(codes)} 个兑换码",
                "error": None,
            }

        except Exception:
            await db_session.rollback()
            logger.exception("批量更新兑换码失败")
            return {
                "success": False,
                "message": None,
                "error": "批量更新失败，请稍后重试",
            }

    async def reassign_record_team(
        self,
        record_id: int,
        new_team_id: int,
        db_session: AsyncSession,
    ) -> Dict[str, Any]:
        """
        修改兑换记录绑定的 Team ID。

        用于管理员手动将用户从旧 Team 移到新 Team 后，同步修正兑换记录
        和兑换码上记录的 Team ID，避免后续质保补车时产生偏移。

        操作内容：
        1. 更新 RedemptionRecord.team_id
        2. 更新对应 RedemptionInviteMarker（如存在）
        3. 通过 _rebuild_code_usage_state 重建兑换码的 used_team_id

        Args:
            record_id: 使用记录 ID
            new_team_id: 新的 Team ID
            db_session: 数据库会话

        Returns:
            结果字典
        """
        try:
            from app.models import RedemptionInviteMarker

            # 1. 查询记录
            stmt = (
                select(RedemptionRecord)
                .where(RedemptionRecord.id == record_id)
                .options(selectinload(RedemptionRecord.redemption_code))
            )
            result = await db_session.execute(stmt)
            record = result.scalar_one_or_none()

            if not record:
                return {"success": False, "error": f"记录 ID {record_id} 不存在"}

            old_team_id = record.team_id

            if old_team_id == new_team_id:
                return {"success": False, "error": "新 Team ID 与当前相同，无需修改"}

            # 2. 验证新 Team 存在
            new_team = await db_session.get(Team, new_team_id)
            if not new_team:
                return {
                    "success": False,
                    "error": f"Team ID {new_team_id} 不存在",
                }

            if new_team.deleted_at is not None:
                return {
                    "success": False,
                    "error": f"Team ID {new_team_id} 已被删除",
                }

            # 3. 检查唯一约束冲突（code + team_id + email）
            conflict_stmt = select(RedemptionRecord).where(
                RedemptionRecord.code == record.code,
                RedemptionRecord.team_id == new_team_id,
                RedemptionRecord.email == record.email,
            )
            conflict_result = await db_session.execute(conflict_stmt)
            if conflict_result.scalar_one_or_none():
                return {
                    "success": False,
                    "error": (
                        f"该邮箱 ({record.email}) 在 Team {new_team_id} "
                        f"已有同一兑换码的记录，无法重复绑定"
                    ),
                }

            # 4. 更新记录的 team_id
            record.team_id = new_team_id

            # 4. 更新关联的 RedemptionInviteMarker（如有）
            marker_stmt = select(RedemptionInviteMarker).where(
                RedemptionInviteMarker.code == record.code,
                RedemptionInviteMarker.team_id == old_team_id,
                RedemptionInviteMarker.email == record.email,
            )
            marker_result = await db_session.execute(marker_stmt)
            marker = marker_result.scalar_one_or_none()
            if marker:
                # 检查新组合是否已存在（唯一约束: code + team_id + email）
                existing_marker_stmt = select(RedemptionInviteMarker).where(
                    RedemptionInviteMarker.code == record.code,
                    RedemptionInviteMarker.team_id == new_team_id,
                    RedemptionInviteMarker.email == record.email,
                )
                existing_marker_result = await db_session.execute(existing_marker_stmt)
                if existing_marker_result.scalar_one_or_none():
                    # 新 Team 已有标记，删除旧的即可
                    await db_session.delete(marker)
                else:
                    marker.team_id = new_team_id

            # 5. 重建兑换码的使用状态（used_team_id 等）
            code = record.redemption_code
            if code:
                await self._rebuild_code_usage_state(db_session, code)

            await db_session.commit()

            new_team_name = new_team.team_name or f"Team {new_team_id}"
            logger.info(
                "管理员修改兑换记录 %s 的 Team: %s -> %s (%s), 邮箱: %s, 兑换码: %s",
                record_id,
                old_team_id,
                new_team_id,
                new_team_name,
                record.email,
                record.code,
            )

            return {
                "success": True,
                "message": (
                    f"已将记录 #{record_id} 的 Team 从 {old_team_id} "
                    f"修改为 {new_team_id} ({new_team_name})"
                ),
            }

        except Exception:
            await db_session.rollback()
            logger.exception("修改兑换记录 Team 失败")
            return {"success": False, "error": "修改失败，请稍后重试"}

    async def get_stats(
        self, db_session: AsyncSession, pool_type: Optional[str] = "normal"
    ) -> Dict[str, int]:
        """
        获取兑换码统计信息

        Returns:
            统计字典, 包含 total, unused, used, expired
        """
        try:
            await self._sync_pool_code_statuses(db_session, pool_type)

            # 使用 SQL 聚合统计各状态数量
            stmt = select(
                RedemptionCode.status, func.count(RedemptionCode.id)
            ).group_by(RedemptionCode.status)
            if pool_type:
                stmt = stmt.where(RedemptionCode.pool_type == pool_type)

            result = await db_session.execute(stmt)
            status_counts = dict(result.all())

            # 由于 "used" 和 "warranty_active" 都属于广义上的 "已使用"
            # 这里的 used 统计需要合并这两个状态
            used_count = status_counts.get("used", 0) + status_counts.get(
                "warranty_active", 0
            )

            # 计算总数
            total_stmt = select(func.count(RedemptionCode.id))
            if pool_type:
                total_stmt = total_stmt.where(RedemptionCode.pool_type == pool_type)
            total_result = await db_session.execute(total_stmt)
            total = total_result.scalar() or 0

            return {
                "total": total,
                "unused": status_counts.get("unused", 0),
                "used": used_count,
                "warranty_active": status_counts.get("warranty_active", 0),
                "expired": status_counts.get("expired", 0),
            }
        except Exception as e:
            logger.error(f"获取兑换码统计信息失败: {e}")
            return {"total": 0, "unused": 0, "used": 0, "expired": 0}


# 创建全局兑换码服务实例
redemption_service = RedemptionService()
