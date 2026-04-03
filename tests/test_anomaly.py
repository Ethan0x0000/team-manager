import unittest

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.database import Base
from app.models import RedemptionCode, RedemptionRecord, Team
from app.services.anomaly import AnomalyService


class StubTeamService:
    def __init__(self, members):
        self._members = members

    async def get_team_members(self, team_id, db_session):
        return {
            "success": True,
            "members": list(self._members),
            "total": len(self._members),
            "error": None,
        }


class AnomalyServiceTests(unittest.IsolatedAsyncioTestCase):
    def __init__(self, methodName: str = "runTest"):
        super().__init__(methodName)
        self.engine: AsyncEngine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    async def asyncSetUp(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.execute(__import__("sqlalchemy").text("PRAGMA foreign_keys=ON"))

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def _seed_team(self):
        async with self.session_factory() as session:
            team = Team(
                id=1,
                email="owner@example.com",
                access_token_encrypted="token-1",
                account_id="acct-1",
                team_name="Case Team",
                status="active",
                current_members=1,
                max_members=5,
                pool_type="normal",
            )
            session.add(team)
            await session.commit()

    async def _seed_binding_record(self, email: str):
        async with self.session_factory() as session:
            code = RedemptionCode(
                code="CASE-CODE-001",
                status="used",
                used_by_email=email,
                used_team_id=1,
                pool_type="normal",
            )
            record = RedemptionRecord(
                email=email,
                code="CASE-CODE-001",
                team_id=1,
                account_id="acct-1",
            )
            session.add_all([code, record])
            await session.commit()

    async def _run_detection(self, member_email: str):
        service = AnomalyService()
        team_service = StubTeamService(
            [
                {
                    "user_id": "user-1",
                    "email": member_email,
                    "role": "standard-user",
                    "added_at": "2026-01-01T00:00:00Z",
                    "status": "joined",
                }
            ]
        )

        async with self.session_factory() as session:
            events = [
                event async for event in service.detect_anomalies(session, team_service)
            ]

        return next(event for event in events if event.get("type") == "finish")

    async def test_detect_anomalies_ignores_case_when_member_is_lower_binding_is_upper(
        self,
    ):
        await self._seed_team()
        await self._seed_binding_record("USER@Example.com")

        finish = await self._run_detection("user@example.com")

        self.assertEqual(finish["total_members_checked"], 1)
        self.assertEqual(finish["anomalies"], [])

    async def test_detect_anomalies_ignores_case_when_member_is_upper_binding_is_lower(
        self,
    ):
        await self._seed_team()
        await self._seed_binding_record("user@example.com")

        finish = await self._run_detection("User@Example.com")

        self.assertEqual(finish["total_members_checked"], 1)
        self.assertEqual(finish["anomalies"], [])

    async def test_detect_anomalies_keeps_anomaly_when_binding_missing(self):
        await self._seed_team()

        finish = await self._run_detection("missing@example.com")

        self.assertEqual(finish["total_members_checked"], 1)
        self.assertEqual(len(finish["anomalies"]), 1)
        self.assertEqual(finish["anomalies"][0]["email"], "missing@example.com")

    async def test_detect_anomalies_matches_binding_with_outer_spaces(self):
        await self._seed_team()
        await self._seed_binding_record("  USER@Example.com  ")

        finish = await self._run_detection("user@example.com")

        self.assertEqual(finish["total_members_checked"], 1)
        self.assertEqual(finish["anomalies"], [])

    async def test_detect_anomalies_preserves_original_member_email_in_payload(self):
        await self._seed_team()

        finish = await self._run_detection("Mixed.Case@Example.com")

        self.assertEqual(finish["total_members_checked"], 1)
        self.assertEqual(len(finish["anomalies"]), 1)
        self.assertEqual(
            finish["anomalies"][0]["email"],
            "Mixed.Case@Example.com",
        )
