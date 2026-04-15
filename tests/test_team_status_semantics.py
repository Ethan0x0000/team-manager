import unittest
from datetime import timedelta
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.database import Base
from app.models import RedemptionCode, RedemptionRecord, Team
from app.services.team import TeamService
from app.services.warranty import WarrantyService, _query_rate_limit
from app.utils.time_utils import get_now


class TeamStatusSemanticsTests(unittest.IsolatedAsyncioTestCase):
    engine: Any = None
    session_factory: Any = None

    async def asyncSetUp(self):
        _query_rate_limit.clear()
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
        _query_rate_limit.clear()
        await self.engine.dispose()

    async def test_get_all_teams_treats_future_expired_status_as_banned(self):
        future_expiry = get_now() + timedelta(days=14)
        past_expiry = get_now() - timedelta(days=1)

        async with self.session_factory() as session:
            session.add_all(
                [
                    Team(
                        id=1,
                        email="future-expired@example.com",
                        access_token_encrypted="token-1",
                        account_id="acct-1",
                        team_name="Future Expired Team",
                        current_members=1,
                        max_members=5,
                        status="expired",
                        expires_at=future_expiry,
                        pool_type="normal",
                    ),
                    Team(
                        id=2,
                        email="past-expired@example.com",
                        access_token_encrypted="token-2",
                        account_id="acct-2",
                        team_name="Past Expired Team",
                        current_members=1,
                        max_members=5,
                        status="expired",
                        expires_at=past_expiry,
                        pool_type="normal",
                    ),
                ]
            )
            await session.commit()

            service = TeamService()

            all_result = await service.get_all_teams(session, status="all")
            teams_by_name = {team["team_name"]: team for team in all_result["teams"]}

            self.assertEqual(teams_by_name["Future Expired Team"]["status"], "banned")
            self.assertEqual(teams_by_name["Past Expired Team"]["status"], "expired")

            banned_result = await service.get_all_teams(session, status="banned")
            banned_names = {team["team_name"] for team in banned_result["teams"]}
            self.assertIn("Future Expired Team", banned_names)
            self.assertNotIn("Past Expired Team", banned_names)

            expired_result = await service.get_all_teams(session, status="expired")
            expired_names = {team["team_name"] for team in expired_result["teams"]}
            self.assertIn("Past Expired Team", expired_names)
            self.assertNotIn("Future Expired Team", expired_names)

    async def test_warranty_reuse_accepts_future_expired_team_as_blocked_history(self):
        async with self.session_factory() as session:
            session.add(
                Team(
                    id=10,
                    email="blocked-owner@example.com",
                    access_token_encrypted="token-10",
                    account_id="acct-10",
                    team_name="Blocked Team",
                    current_members=1,
                    max_members=5,
                    status="expired",
                    expires_at=get_now() + timedelta(days=21),
                    pool_type="normal",
                )
            )
            await session.commit()

            session.add_all(
                [
                    RedemptionCode(
                        code="WARRANTY-BLOCKED-001",
                        status="used",
                        has_warranty=True,
                        warranty_days=30,
                        used_by_email="buyer@example.com",
                        used_team_id=10,
                        used_at=get_now() - timedelta(days=2),
                        warranty_expires_at=get_now() + timedelta(days=28),
                    ),
                    RedemptionRecord(
                        email="buyer@example.com",
                        code="WARRANTY-BLOCKED-001",
                        team_id=10,
                        account_id="acct-10",
                    ),
                ]
            )
            await session.commit()

            service = WarrantyService()
            result = await service.validate_warranty_reuse(
                session,
                "WARRANTY-BLOCKED-001",
                "buyer@example.com",
            )

            self.assertTrue(result["success"])
            self.assertTrue(result["can_reuse"])
            self.assertIn("之前加入的 Team 已不可用", result["reason"])

    async def test_warranty_check_reports_future_expired_team_as_banned(self):
        async with self.session_factory() as session:
            session.add(
                Team(
                    id=20,
                    email="blocked-owner-2@example.com",
                    access_token_encrypted="token-20",
                    account_id="acct-20",
                    team_name="Blocked Warranty Team",
                    current_members=1,
                    max_members=5,
                    status="expired",
                    expires_at=get_now() + timedelta(days=10),
                    pool_type="normal",
                )
            )
            await session.commit()

            session.add_all(
                [
                    RedemptionCode(
                        code="WARRANTY-BLOCKED-002",
                        status="used",
                        has_warranty=True,
                        warranty_days=30,
                        used_by_email="buyer@example.com",
                        used_team_id=20,
                        used_at=get_now() - timedelta(days=1),
                        warranty_expires_at=get_now() + timedelta(days=29),
                    ),
                    RedemptionRecord(
                        email="buyer@example.com",
                        code="WARRANTY-BLOCKED-002",
                        team_id=20,
                        account_id="acct-20",
                    ),
                ]
            )
            await session.commit()

            service = WarrantyService()
            result = await service.check_warranty_status(
                session, code="WARRANTY-BLOCKED-002"
            )

            self.assertTrue(result["success"])
            self.assertTrue(result["can_reuse"])
            self.assertEqual(len(result["banned_teams"]), 1)
            self.assertEqual(result["banned_teams"][0]["team_id"], 20)
            self.assertEqual(result["records"][0]["team_status"], "banned")

    async def test_access_failure_classification_distinguishes_real_expiry(self):
        future_team = Team(
            id=30,
            email="future-failure@example.com",
            access_token_encrypted="token-30",
            account_id="acct-30",
            team_name="Future Failure Team",
            current_members=1,
            max_members=5,
            status="active",
            expires_at=get_now() + timedelta(days=7),
            pool_type="normal",
        )
        expired_team = Team(
            id=31,
            email="expired-failure@example.com",
            access_token_encrypted="token-31",
            account_id="acct-31",
            team_name="Expired Failure Team",
            current_members=1,
            max_members=5,
            status="active",
            expires_at=get_now() - timedelta(days=1),
            pool_type="normal",
        )

        self.assertEqual(TeamService._status_for_access_failure(future_team), "banned")
        self.assertEqual(
            TeamService._status_for_access_failure(expired_team), "expired"
        )
