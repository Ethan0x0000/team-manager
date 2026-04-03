import unittest
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
)
from app.services.redeem_flow import RedeemFlowService
from app.services.redemption import RedemptionService
from app.utils.time_utils import get_now


class StubRedemptionService:
    async def validate_code(self, code: str, db_session: AsyncSession):
        return {
            "success": True,
            "valid": True,
            "redemption_code": {
                "pool_type": "normal",
                "virtual_welfare_code": False,
            },
        }


class StubTeamService:
    async def reserve_seat_if_available(self, team_id, db_session, pool_type="normal"):
        team = await db_session.get(Team, team_id)
        if not team or team.pool_type != pool_type or team.status != "active":
            return {"success": False, "error": "Team 不可用"}
        if team.current_members >= team.max_members:
            team.status = "full"
            return {"success": False, "error": "该 Team 已满, 请选择其他 Team 尝试"}

        team.current_members += 1
        if team.current_members >= team.max_members:
            team.status = "full"
        return {"success": True, "team": team, "error": None}

    async def release_reserved_seat(self, team_id, db_session, pool_type="normal"):
        team = await db_session.get(Team, team_id)
        if team and team.current_members > 0:
            team.current_members -= 1
            team.status = "active"

    async def ensure_access_token(self, team, db_session):
        return "token"

    async def get_active_team_ids_for_email(self, email, db_session, pool_type=None):
        return []

    async def upsert_team_email_mapping(
        self, team_id, email, status, db_session, source="sync", seen_at=None
    ):
        return None


class StubChatGPTService:
    def __init__(self):
        self.results = [
            {
                "success": True,
                "data": {
                    "account_invites": [{"email": "user@example.com"}],
                },
            },
            {"success": False, "error": "Already in workspace"},
        ]

    async def send_invite(
        self, access_token, account_id, email, db_session, identifier="default"
    ):
        if self.results:
            return self.results.pop(0)
        return {"success": True, "data": {"account_invites": [{"email": email}]}}


class SequenceChatGPTService:
    def __init__(self, results):
        self.results = list(results)

    async def send_invite(
        self, access_token, account_id, email, db_session, identifier="default"
    ):
        if self.results:
            return self.results.pop(0)
        return {"success": False, "error": "Already in workspace"}


class RedeemFlowRegressionTests(unittest.IsolatedAsyncioTestCase):
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

    @staticmethod
    def _close_coro(coro):
        coro.close()
        return None

    async def _seed_data(self):
        async with self.session_factory() as session:
            team = Team(
                id=1,
                email="owner-1@example.com",
                access_token_encrypted="token-1",
                account_id="acct-1",
                team_name="Team 1",
                current_members=3,
                max_members=6,
                status="active",
                pool_type="normal",
            )
            code = RedemptionCode(
                code="TEST-CODE-0001",
                status="unused",
                pool_type="normal",
                reusable_by_seat=False,
            )
            session.add_all([team, code])
            await session.commit()

    async def _seed_code_with_record(
        self,
        *,
        team_id: int,
        team_status: str,
        code: str,
        email: str,
    ) -> Any:
        async with self.session_factory() as session:
            team = Team(
                id=team_id,
                email=f"owner-{team_id}@example.com",
                access_token_encrypted=f"token-{team_id}",
                account_id=f"acct-{team_id}",
                team_name=f"Team {team_id}",
                current_members=1,
                max_members=6,
                status=team_status,
                pool_type="normal",
            )
            session.add(team)
            await session.commit()

            redeem_code = RedemptionCode(
                code=code,
                status="used",
                pool_type="normal",
                reusable_by_seat=False,
                used_by_email=email,
                used_team_id=team_id,
                used_at=get_now(),
            )
            session.add(redeem_code)
            await session.commit()

            record = RedemptionRecord(
                email=email,
                code=code,
                team_id=team_id,
                account_id=f"acct-{team_id}",
            )
            session.add(record)
            await session.commit()
            return record.id

    @staticmethod
    def _invite_success_payload(email: str = "user@example.com"):
        return {
            "success": True,
            "data": {
                "account_invites": [{"email": email}],
            },
        }

    async def test_retry_after_post_invite_persist_failure_is_idempotently_recovered(
        self,
    ):
        await self._seed_data()

        service = RedeemFlowService()
        service.__dict__["redemption_service"] = StubRedemptionService()
        service.__dict__["team_service"] = StubTeamService()
        service.__dict__["chatgpt_service"] = StubChatGPTService()

        original_upsert = service.team_service.upsert_team_email_mapping
        failure_state = {"raised": False}

        async def flaky_upsert(
            team_id, email, status, db_session, source="sync", seen_at=None
        ):
            if not failure_state["raised"]:
                failure_state["raised"] = True
                raise RuntimeError("database is locked")
            return await original_upsert(
                team_id,
                email,
                status,
                db_session,
                source,
                seen_at,
            )

        async with self.session_factory() as session:
            with (
                patch(
                    "app.services.redeem_flow.asyncio.create_task",
                    side_effect=self._close_coro,
                ),
                patch.object(
                    service.team_service,
                    "upsert_team_email_mapping",
                    side_effect=flaky_upsert,
                ),
            ):
                result = await service.redeem_and_join_team(
                    email="user@example.com",
                    code="TEST-CODE-0001",
                    team_id=1,
                    db_session=session,
                )

            self.assertTrue(result["success"])
            self.assertEqual(result["team_info"]["id"], 1)

            code = await session.get(RedemptionCode, 1)
            self.assertIsNotNone(code)
            assert code is not None
            self.assertEqual(code.status, "used")
            self.assertEqual(code.used_team_id, 1)

            records = (
                (
                    await session.execute(
                        select(RedemptionRecord).where(
                            RedemptionRecord.code == "TEST-CODE-0001",
                            RedemptionRecord.email == "user@example.com",
                            RedemptionRecord.team_id == 1,
                        )
                    )
                )
                .scalars()
                .all()
            )
            self.assertEqual(len(records), 1)

    async def test_redeem_normalizes_email_for_record_and_code(self):
        await self._seed_data()

        service = RedeemFlowService()
        service.__dict__["redemption_service"] = StubRedemptionService()
        service.__dict__["team_service"] = StubTeamService()
        service.__dict__["chatgpt_service"] = SequenceChatGPTService(
            [self._invite_success_payload("User@Example.com")]
        )

        async with self.session_factory() as session:
            with patch(
                "app.services.redeem_flow.asyncio.create_task",
                side_effect=self._close_coro,
            ):
                result = await service.redeem_and_join_team(
                    email=" User@Example.com ",
                    code="TEST-CODE-0001",
                    team_id=1,
                    db_session=session,
                )

            self.assertTrue(result["success"])

            code = await session.get(RedemptionCode, 1)
            self.assertIsNotNone(code)
            assert code is not None
            self.assertEqual(code.used_by_email, "user@example.com")

            records = (
                (
                    await session.execute(
                        select(RedemptionRecord).where(
                            RedemptionRecord.code == "TEST-CODE-0001",
                            RedemptionRecord.team_id == 1,
                        )
                    )
                )
                .scalars()
                .all()
            )
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0].email, "user@example.com")

    async def test_new_request_recovers_with_persisted_invite_marker(self):
        await self._seed_data()

        first_service = RedeemFlowService()
        first_service.__dict__["redemption_service"] = StubRedemptionService()
        first_service.__dict__["team_service"] = StubTeamService()
        first_service.__dict__["chatgpt_service"] = SequenceChatGPTService(
            [
                self._invite_success_payload(),
                {"success": False, "error": "Already in workspace"},
            ]
        )

        async with self.session_factory() as session:
            with (
                patch(
                    "app.services.redeem_flow.asyncio.create_task",
                    side_effect=self._close_coro,
                ),
                patch(
                    "app.services.redeem_flow.asyncio.sleep",
                    new=AsyncMock(return_value=None),
                ),
                patch.object(
                    first_service,
                    "_persist_success_state",
                    new=AsyncMock(side_effect=RuntimeError("database is locked")),
                ),
            ):
                failed_result = await first_service.redeem_and_join_team(
                    email="user@example.com",
                    code="TEST-CODE-0001",
                    team_id=1,
                    db_session=session,
                )

            self.assertFalse(failed_result["success"])
            self.assertIn("兑换失败次数过多", failed_result["error"])

            marker_rows = (
                (
                    await session.execute(
                        select(RedemptionInviteMarker).where(
                            RedemptionInviteMarker.code == "TEST-CODE-0001",
                            RedemptionInviteMarker.team_id == 1,
                            RedemptionInviteMarker.email == "user@example.com",
                        )
                    )
                )
                .scalars()
                .all()
            )
            self.assertEqual(len(marker_rows), 1)

            records_after_first_request = (
                (
                    await session.execute(
                        select(RedemptionRecord).where(
                            RedemptionRecord.code == "TEST-CODE-0001",
                            RedemptionRecord.team_id == 1,
                            RedemptionRecord.email == "user@example.com",
                        )
                    )
                )
                .scalars()
                .all()
            )
            self.assertEqual(records_after_first_request, [])

        second_service = RedeemFlowService()
        second_service.__dict__["redemption_service"] = StubRedemptionService()
        second_service.__dict__["team_service"] = StubTeamService()
        second_service.__dict__["chatgpt_service"] = SequenceChatGPTService(
            [{"success": False, "error": "Already in workspace"}]
        )

        async with self.session_factory() as session:
            with patch(
                "app.services.redeem_flow.asyncio.create_task",
                side_effect=self._close_coro,
            ):
                recovered_result = await second_service.redeem_and_join_team(
                    email="user@example.com",
                    code="TEST-CODE-0001",
                    team_id=1,
                    db_session=session,
                )

            self.assertTrue(recovered_result["success"])
            self.assertEqual(recovered_result["team_info"]["id"], 1)

            recovered_records = (
                (
                    await session.execute(
                        select(RedemptionRecord).where(
                            RedemptionRecord.code == "TEST-CODE-0001",
                            RedemptionRecord.team_id == 1,
                            RedemptionRecord.email == "user@example.com",
                        )
                    )
                )
                .scalars()
                .all()
            )
            self.assertEqual(len(recovered_records), 1)

            refreshed_code = await session.get(RedemptionCode, 1)
            self.assertIsNotNone(refreshed_code)
            assert refreshed_code is not None
            self.assertEqual(refreshed_code.status, "used")
            self.assertEqual(refreshed_code.used_team_id, 1)

    async def test_withdraw_record_allows_local_cleanup_when_team_banned(self):
        record_id = await self._seed_code_with_record(
            team_id=2,
            team_status="banned",
            code="LOCK-CODE-0001",
            email="buyer@example.com",
        )

        service = RedemptionService()

        async with self.session_factory() as session:
            with patch(
                "app.services.team.team_service.remove_invite_or_member",
                new=AsyncMock(
                    return_value={
                        "success": False,
                        "message": None,
                        "error": "账号已封禁 (account_deactivated)",
                    }
                ),
            ):
                withdraw_result = await service.withdraw_record(record_id, session)

            self.assertTrue(withdraw_result["success"])

            removed_record = await session.get(RedemptionRecord, record_id)
            self.assertIsNone(removed_record)

            code_result = await session.execute(
                select(RedemptionCode).where(RedemptionCode.code == "LOCK-CODE-0001")
            )
            refreshed_code = code_result.scalar_one_or_none()
            self.assertIsNotNone(refreshed_code)
            assert refreshed_code is not None
            self.assertEqual(refreshed_code.status, "unused")
            self.assertIsNone(refreshed_code.used_by_email)
            self.assertIsNone(refreshed_code.used_team_id)

            delete_result = await service.delete_code("LOCK-CODE-0001", session)
            self.assertTrue(delete_result["success"])

    async def test_withdraw_record_still_blocks_on_active_team_remote_error(self):
        record_id = await self._seed_code_with_record(
            team_id=3,
            team_status="active",
            code="LOCK-CODE-0002",
            email="buyer@example.com",
        )

        service = RedemptionService()

        async with self.session_factory() as session:
            with patch(
                "app.services.team.team_service.remove_invite_or_member",
                new=AsyncMock(
                    return_value={
                        "success": False,
                        "message": None,
                        "error": "temporary upstream failure",
                    }
                ),
            ):
                withdraw_result = await service.withdraw_record(record_id, session)

            self.assertFalse(withdraw_result["success"])
            self.assertIn("从 Team 移除成员失败", withdraw_result["error"])

            existing_record = await session.get(RedemptionRecord, record_id)
            self.assertIsNotNone(existing_record)

            delete_result = await service.delete_code("LOCK-CODE-0002", session)
            self.assertFalse(delete_result["success"])
            self.assertIn("无法直接删除", delete_result["error"])
