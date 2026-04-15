import unittest
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, patch

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.database import Base
from app.models import RedemptionCode, RedemptionInviteMarker, RedemptionRecord, Team
from app.services.redemption import RedemptionService
from app.services.team import team_service
from app.utils.time_utils import get_now


class InvalidCodeScanTests(unittest.IsolatedAsyncioTestCase):
    engine: Any = None
    session_factory: Any = None

    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.session_factory = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.execute(__import__("sqlalchemy").text("PRAGMA foreign_keys=ON"))

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def _seed_expired_code(
        self,
        *,
        code: str,
        records: list[dict[str, Any]],
        expires_at=None,
    ) -> None:
        async with self.session_factory() as session:
            for item in records:
                team = Team(
                    id=item["team_id"],
                    email=f"owner-{item['team_id']}@example.com",
                    access_token_encrypted=f"token-{item['team_id']}",
                    account_id=f"acct-{item['team_id']}",
                    team_name=f"Team {item['team_id']}",
                    current_members=item.get("current_members", 1),
                    max_members=item.get("max_members", 5),
                    status=item["team_status"],
                    expires_at=item.get("team_expires_at"),
                    pool_type="normal",
                    deleted_at=item.get("deleted_at"),
                )
                session.add(team)

            await session.commit()

            latest = max(
                records,
                key=lambda item: (item["redeemed_at"], item["team_id"]),
            )
            session.add(
                RedemptionCode(
                    code=code,
                    status="expired",
                    expires_at=expires_at or (get_now() - timedelta(days=1)),
                    pool_type="normal",
                    reusable_by_seat=False,
                    used_by_email=latest["email"],
                    used_team_id=latest["team_id"],
                    used_at=latest["redeemed_at"],
                )
            )

            for item in records:
                session.add(
                    RedemptionRecord(
                        email=item["email"],
                        code=code,
                        team_id=item["team_id"],
                        account_id=f"acct-{item['team_id']}",
                        redeemed_at=item["redeemed_at"],
                    )
                )

            await session.commit()

    async def test_scan_removes_email_from_last_normal_team_before_marking_code_deletable(
        self,
    ):
        redeemed_at = get_now() - timedelta(days=2)
        await self._seed_expired_code(
            code="EXPIRED-ACTIVE-0001",
            records=[
                {
                    "team_id": 11,
                    "team_status": "active",
                    "email": "buyer@example.com",
                    "redeemed_at": redeemed_at,
                }
            ],
        )

        service = RedemptionService()
        async with self.session_factory() as session:
            with patch(
                "app.services.team.team_service.remove_invite_or_member",
                new=AsyncMock(return_value={"success": True, "message": "成员已删除"}),
            ) as remove_mock:
                result = await service.get_invalid_code_candidates(session)

        self.assertTrue(result["success"])
        self.assertEqual(result["total"], 1)
        candidate = result["codes"][0]
        self.assertEqual(candidate["code"], "EXPIRED-ACTIVE-0001")
        self.assertEqual(candidate["last_team_id"], 11)
        self.assertEqual(candidate["last_team_status"], "active")
        self.assertEqual(candidate["cleanup_action"], "removed_from_normal_team")
        remove_mock.assert_awaited_once_with(11, "buyer@example.com", session)

    async def test_scan_marks_code_deletable_when_last_team_already_invalid(self):
        redeemed_at = get_now() - timedelta(days=3)
        await self._seed_expired_code(
            code="EXPIRED-BANNED-0001",
            records=[
                {
                    "team_id": 12,
                    "team_status": "banned",
                    "email": "buyer@example.com",
                    "redeemed_at": redeemed_at,
                }
            ],
        )

        service = RedemptionService()
        async with self.session_factory() as session:
            with patch(
                "app.services.team.team_service.remove_invite_or_member",
                new=AsyncMock(return_value={"success": True}),
            ) as remove_mock:
                result = await service.get_invalid_code_candidates(session)

        self.assertTrue(result["success"])
        self.assertEqual(result["total"], 1)
        candidate = result["codes"][0]
        self.assertEqual(candidate["cleanup_action"], "team_already_invalid")
        self.assertEqual(candidate["last_team_status"], "banned")
        remove_mock.assert_not_awaited()

    async def test_scan_reports_email_already_missing_when_normal_team_has_no_member(
        self,
    ):
        redeemed_at = get_now() - timedelta(days=3)
        await self._seed_expired_code(
            code="EXPIRED-MISSING-0001",
            records=[
                {
                    "team_id": 13,
                    "team_status": "active",
                    "email": "buyer@example.com",
                    "redeemed_at": redeemed_at,
                }
            ],
        )

        service = RedemptionService()
        async with self.session_factory() as session:
            with patch(
                "app.services.team.team_service.remove_invite_or_member",
                new=AsyncMock(
                    return_value={"success": True, "message": "成员已不存在"}
                ),
            ):
                result = await service.get_invalid_code_candidates(session)

        self.assertTrue(result["success"])
        self.assertEqual(result["total"], 1)
        candidate = result["codes"][0]
        self.assertEqual(
            candidate["cleanup_action"], "email_already_missing_from_normal_team"
        )

    async def test_scan_uses_only_latest_redemption_record_for_team_cleanup(self):
        older = get_now() - timedelta(days=10)
        latest = get_now() - timedelta(days=1)
        await self._seed_expired_code(
            code="EXPIRED-LATEST-0001",
            records=[
                {
                    "team_id": 20,
                    "team_status": "banned",
                    "email": "old@example.com",
                    "redeemed_at": older,
                },
                {
                    "team_id": 21,
                    "team_status": "full",
                    "email": "latest@example.com",
                    "redeemed_at": latest,
                },
            ],
        )

        service = RedemptionService()
        async with self.session_factory() as session:
            with patch(
                "app.services.team.team_service.remove_invite_or_member",
                new=AsyncMock(return_value={"success": True, "message": "成员已删除"}),
            ) as remove_mock:
                result = await service.get_invalid_code_candidates(session)

        self.assertTrue(result["success"])
        self.assertEqual(result["total"], 1)
        candidate = result["codes"][0]
        self.assertEqual(candidate["last_team_id"], 21)
        self.assertEqual(candidate["last_team_status"], "full")
        remove_mock.assert_awaited_once_with(21, "latest@example.com", session)

    async def test_cleanup_invalid_codes_deletes_code_after_latest_record_screening(
        self,
    ):
        redeemed_at = get_now() - timedelta(days=4)
        await self._seed_expired_code(
            code="EXPIRED-CLEANUP-0001",
            records=[
                {
                    "team_id": 31,
                    "team_status": "active",
                    "email": "cleanup@example.com",
                    "redeemed_at": redeemed_at,
                }
            ],
        )

        service = RedemptionService()
        async with self.session_factory() as session:
            with patch(
                "app.services.team.team_service.remove_invite_or_member",
                new=AsyncMock(return_value={"success": True, "message": "成员已删除"}),
            ):
                result = await service.cleanup_invalid_codes(
                    ["EXPIRED-CLEANUP-0001"], session
                )

            self.assertTrue(result["success"])
            self.assertEqual(result["deleted_codes"], ["EXPIRED-CLEANUP-0001"])

            code_result = await session.execute(
                select(RedemptionCode).where(
                    RedemptionCode.code == "EXPIRED-CLEANUP-0001"
                )
            )
            self.assertIsNone(code_result.scalar_one_or_none())

            record_result = await session.execute(
                select(RedemptionRecord).where(
                    RedemptionRecord.code == "EXPIRED-CLEANUP-0001"
                )
            )
            self.assertEqual(record_result.scalars().all(), [])

    async def test_cleanup_invalid_codes_does_not_mutate_unselected_expired_codes(self):
        redeemed_at = get_now() - timedelta(days=5)
        await self._seed_expired_code(
            code="EXPIRED-CLEANUP-TARGET",
            records=[
                {
                    "team_id": 41,
                    "team_status": "active",
                    "email": "target@example.com",
                    "redeemed_at": redeemed_at,
                }
            ],
        )
        await self._seed_expired_code(
            code="EXPIRED-CLEANUP-OTHER",
            records=[
                {
                    "team_id": 42,
                    "team_status": "active",
                    "email": "other@example.com",
                    "redeemed_at": redeemed_at,
                }
            ],
        )

        service = RedemptionService()
        async with self.session_factory() as session:
            with patch(
                "app.services.team.team_service.remove_invite_or_member",
                new=AsyncMock(return_value={"success": True, "message": "成员已删除"}),
            ) as remove_mock:
                result = await service.cleanup_invalid_codes(
                    ["EXPIRED-CLEANUP-TARGET"], session
                )

        self.assertTrue(result["success"])
        self.assertEqual(result["deleted_codes"], ["EXPIRED-CLEANUP-TARGET"])
        self.assertFalse(
            any(call.args[0] == 42 for call in remove_mock.await_args_list),
            "cleanup_invalid_codes should not mutate unrelated expired codes",
        )

    async def test_cleanup_invalid_codes_deletes_invite_markers(self):
        redeemed_at = get_now() - timedelta(days=4)
        await self._seed_expired_code(
            code="EXPIRED-MARKER-0001",
            records=[
                {
                    "team_id": 51,
                    "team_status": "active",
                    "email": "marker@example.com",
                    "redeemed_at": redeemed_at,
                }
            ],
        )

        async with self.session_factory() as session:
            session.add(
                RedemptionInviteMarker(
                    code="EXPIRED-MARKER-0001",
                    team_id=51,
                    email="marker@example.com",
                )
            )
            await session.commit()

            service = RedemptionService()
            with patch(
                "app.services.team.team_service.remove_invite_or_member",
                new=AsyncMock(return_value={"success": True, "message": "成员已删除"}),
            ):
                result = await service.cleanup_invalid_codes(
                    ["EXPIRED-MARKER-0001"], session
                )

            self.assertTrue(result["success"])

            marker_result = await session.execute(
                select(RedemptionInviteMarker).where(
                    RedemptionInviteMarker.code == "EXPIRED-MARKER-0001"
                )
            )
            self.assertEqual(marker_result.scalars().all(), [])

    async def test_remove_invite_or_member_matches_email_case_insensitively(self):
        async with self.session_factory() as session:
            with (
                patch.object(
                    team_service,
                    "get_team_members",
                    new=AsyncMock(
                        return_value={
                            "success": True,
                            "members": [
                                {
                                    "email": "Buyer@Example.com",
                                    "status": "joined",
                                    "user_id": "user-1",
                                }
                            ],
                        }
                    ),
                ),
                patch.object(
                    team_service,
                    "delete_team_member",
                    new=AsyncMock(return_value={"success": True}),
                ) as delete_mock,
                patch.object(
                    team_service,
                    "sync_team_info",
                    new=AsyncMock(return_value={"success": True}),
                ) as sync_mock,
            ):
                result = await team_service.remove_invite_or_member(
                    99, "buyer@example.com", session
                )

        self.assertTrue(result["success"])
        delete_mock.assert_awaited_once_with(
            99, "user-1", session, email="buyer@example.com"
        )
        sync_mock.assert_not_awaited()
