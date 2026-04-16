import unittest
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, patch

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.database import Base
from app.models import (
    RedemptionCode,
    RedemptionInviteMarker,
    RedemptionRecord,
    Team,
    TeamEmailMapping,
)
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
        code_status: str = "expired",
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
                    status=code_status,
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

    async def _seed_team_email_mapping(
        self, *, team_id: int, email: str, status: str
    ) -> None:
        async with self.session_factory() as session:
            session.add(
                TeamEmailMapping(
                    team_id=team_id,
                    email=email.lower(),
                    status=status,
                )
            )
            await session.commit()

    async def test_scan_classifies_last_normal_team_without_removing_email(
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
        await self._seed_team_email_mapping(
            team_id=11, email="buyer@example.com", status="joined"
        )

        service = RedemptionService()
        async with self.session_factory() as session:
            with (
                patch(
                    "app.services.team.team_service.remove_invite_or_member",
                    new=AsyncMock(
                        return_value={"success": True, "message": "成员已删除"}
                    ),
                ) as remove_mock,
            ):
                result = await service.get_invalid_code_candidates(session)

        self.assertTrue(result["success"])
        self.assertEqual(result["total"], 1)
        candidate = result["codes"][0]
        self.assertEqual(candidate["code"], "EXPIRED-ACTIVE-0001")
        self.assertEqual(candidate["last_team_id"], 11)
        self.assertEqual(candidate["last_team_status"], "active")
        self.assertEqual(candidate["cleanup_action"], "requires_team_removal")
        self.assertTrue(candidate["can_remove_from_team"])
        self.assertFalse(candidate["can_delete"])
        remove_mock.assert_not_awaited()

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
        self.assertFalse(candidate["can_remove_from_team"])
        self.assertTrue(candidate["can_delete"])
        remove_mock.assert_not_awaited()

    async def test_scan_marks_code_deletable_when_last_normal_team_email_already_missing(
        self,
    ):
        redeemed_at = get_now() - timedelta(days=2)
        await self._seed_expired_code(
            code="EXPIRED-MISSING-0001",
            records=[
                {
                    "team_id": 14,
                    "team_status": "active",
                    "email": "buyer@example.com",
                    "redeemed_at": redeemed_at,
                }
            ],
        )
        await self._seed_team_email_mapping(
            team_id=14, email="buyer@example.com", status="removed"
        )

        service = RedemptionService()
        async with self.session_factory() as session:
            with (
                patch.object(
                    team_service,
                    "get_team_members",
                    new=AsyncMock(
                        side_effect=AssertionError(
                            "scan should not fetch remote members"
                        )
                    ),
                ) as members_mock,
                patch(
                    "app.services.team.team_service.remove_invite_or_member",
                    new=AsyncMock(
                        return_value={"success": True, "message": "成员已删除"}
                    ),
                ) as remove_mock,
            ):
                result = await service.get_invalid_code_candidates(session)

        self.assertTrue(result["success"])
        self.assertEqual(result["total"], 1)
        candidate = result["codes"][0]
        self.assertEqual(candidate["cleanup_action"], "team_email_already_absent")
        self.assertFalse(candidate["can_remove_from_team"])
        self.assertTrue(candidate["can_delete"])
        members_mock.assert_not_awaited()
        remove_mock.assert_not_awaited()

    async def test_scan_is_read_only_for_code_status_and_member_lookup(self):
        redeemed_at = get_now() - timedelta(days=2)
        await self._seed_expired_code(
            code="EXPIRED-READONLY-0001",
            records=[
                {
                    "team_id": 15,
                    "team_status": "active",
                    "email": "readonly@example.com",
                    "redeemed_at": redeemed_at,
                }
            ],
            code_status="used",
        )
        await self._seed_team_email_mapping(
            team_id=15, email="readonly@example.com", status="joined"
        )

        service = RedemptionService()
        async with self.session_factory() as session:
            with (
                patch.object(
                    service,
                    "_sync_pool_code_statuses",
                    new=AsyncMock(
                        side_effect=AssertionError("scan should not sync statuses")
                    ),
                ) as sync_mock,
                patch.object(
                    team_service,
                    "get_team_members",
                    new=AsyncMock(
                        side_effect=AssertionError(
                            "scan should not fetch remote members"
                        )
                    ),
                ) as members_mock,
            ):
                result = await service.get_invalid_code_candidates(session)

        self.assertTrue(result["success"])
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["codes"][0]["cleanup_action"], "requires_team_removal")
        sync_mock.assert_not_awaited()
        members_mock.assert_not_awaited()

        async with self.session_factory() as session:
            code_result = await session.execute(
                select(RedemptionCode).where(
                    RedemptionCode.code == "EXPIRED-READONLY-0001"
                )
            )
            stored_code = code_result.scalar_one()

        self.assertEqual(
            stored_code.status,
            "used",
            "scan should not rewrite persisted code status while classifying invalid codes",
        )

    async def test_remove_invalid_code_team_members_marks_selected_codes_ready_for_deletion(
        self,
    ):
        redeemed_at = get_now() - timedelta(days=3)
        await self._seed_expired_code(
            code="EXPIRED-REMOVE-0001",
            records=[
                {
                    "team_id": 13,
                    "team_status": "active",
                    "email": "buyer@example.com",
                    "redeemed_at": redeemed_at,
                }
            ],
        )
        await self._seed_team_email_mapping(
            team_id=13, email="buyer@example.com", status="joined"
        )

        service = RedemptionService()
        async with self.session_factory() as session:
            with (
                patch(
                    "app.services.team.team_service.remove_invite_or_member",
                    new=AsyncMock(),
                ) as remove_mock,
            ):

                async def remove_side_effect(team_id, email, db_session):
                    await team_service.mark_team_email_mapping_removed(
                        team_id, email, db_session, source="api"
                    )
                    return {"success": True, "message": "成员已删除"}

                remove_mock.side_effect = remove_side_effect
                result = await service.remove_invalid_code_team_members(
                    ["EXPIRED-REMOVE-0001"], session
                )
                cleanup_result = await service.cleanup_invalid_codes(
                    ["EXPIRED-REMOVE-0001"], session
                )

        self.assertTrue(result["success"])
        self.assertEqual(result["removed_codes"], ["EXPIRED-REMOVE-0001"])
        candidate = result["updated_codes"][0]
        self.assertEqual(
            candidate["cleanup_action"], "ready_to_delete_after_team_removal"
        )
        self.assertFalse(candidate["can_remove_from_team"])
        self.assertTrue(candidate["can_delete"])
        remove_mock.assert_awaited_once_with(13, "buyer@example.com", session)
        self.assertTrue(cleanup_result["success"])
        self.assertEqual(cleanup_result["deleted_codes"], ["EXPIRED-REMOVE-0001"])

    async def test_scan_uses_only_latest_redemption_record_for_classification(self):
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
        await self._seed_team_email_mapping(
            team_id=21, email="latest@example.com", status="joined"
        )

        service = RedemptionService()
        async with self.session_factory() as session:
            with (
                patch(
                    "app.services.team.team_service.remove_invite_or_member",
                    new=AsyncMock(
                        return_value={"success": True, "message": "成员已删除"}
                    ),
                ) as remove_mock,
            ):
                result = await service.get_invalid_code_candidates(session)

        self.assertTrue(result["success"])
        self.assertEqual(result["total"], 1)
        candidate = result["codes"][0]
        self.assertEqual(candidate["last_team_id"], 21)
        self.assertEqual(candidate["last_team_status"], "full")
        self.assertEqual(candidate["cleanup_action"], "requires_team_removal")
        self.assertTrue(candidate["can_remove_from_team"])
        self.assertFalse(candidate["can_delete"])
        remove_mock.assert_not_awaited()

    async def test_cleanup_invalid_codes_requires_team_removal_before_deletion(
        self,
    ):
        redeemed_at = get_now() - timedelta(days=4)
        await self._seed_expired_code(
            code="EXPIRED-CLEANUP-BLOCKED",
            records=[
                {
                    "team_id": 30,
                    "team_status": "active",
                    "email": "cleanup@example.com",
                    "redeemed_at": redeemed_at,
                }
            ],
        )
        await self._seed_team_email_mapping(
            team_id=30, email="cleanup@example.com", status="joined"
        )

        service = RedemptionService()
        async with self.session_factory() as session:
            with (
                patch(
                    "app.services.team.team_service.remove_invite_or_member",
                    new=AsyncMock(
                        return_value={"success": True, "message": "成员已删除"}
                    ),
                ) as remove_mock,
            ):
                result = await service.cleanup_invalid_codes(
                    ["EXPIRED-CLEANUP-BLOCKED"], session
                )

            self.assertFalse(result["success"])
            self.assertIn("移出 Team", result["error"])
            remove_mock.assert_not_awaited()

            code_result = await session.execute(
                select(RedemptionCode).where(
                    RedemptionCode.code == "EXPIRED-CLEANUP-BLOCKED"
                )
            )
            self.assertIsNotNone(code_result.scalar_one_or_none())

    async def test_cleanup_invalid_codes_deletes_code_after_latest_record_screening(
        self,
    ):
        redeemed_at = get_now() - timedelta(days=4)
        await self._seed_expired_code(
            code="EXPIRED-CLEANUP-0001",
            records=[
                {
                    "team_id": 31,
                    "team_status": "banned",
                    "email": "cleanup@example.com",
                    "redeemed_at": redeemed_at,
                }
            ],
        )

        service = RedemptionService()
        async with self.session_factory() as session:
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
                    "team_status": "banned",
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
                    "team_status": "banned",
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
                    "team_status": "banned",
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
