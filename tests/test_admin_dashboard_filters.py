import sys
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from starlette.requests import Request

from app.database import Base
from app.main import app
from app.models import Team
from app.routes import admin as admin_routes


class AdminDashboardStatusFilterTests(unittest.IsolatedAsyncioTestCase):
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

    async def _seed_teams(self):
        async with self.session_factory() as session:
            session.add_all(
                [
                    Team(
                        id=1,
                        email="active-owner@example.com",
                        access_token_encrypted="token-1",
                        account_id="acct-1",
                        team_name="Team Active",
                        current_members=2,
                        max_members=5,
                        status="active",
                        pool_type="normal",
                    ),
                    Team(
                        id=2,
                        email="full-owner@example.com",
                        access_token_encrypted="token-2",
                        account_id="acct-2",
                        team_name="Team Full",
                        current_members=5,
                        max_members=5,
                        status="full",
                        pool_type="normal",
                    ),
                    Team(
                        id=3,
                        email="expired-owner@example.com",
                        access_token_encrypted="token-3",
                        account_id="acct-3",
                        team_name="Team Expired",
                        current_members=1,
                        max_members=5,
                        status="expired",
                        pool_type="normal",
                    ),
                ]
            )
            await session.commit()

    @staticmethod
    def _build_request(query_string: str = "") -> Request:
        scope = {
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": "/admin",
            "raw_path": b"/admin",
            "query_string": query_string.encode("utf-8"),
            "headers": [(b"host", b"testserver")],
            "client": ("testclient", 50000),
            "server": ("testserver", 80),
            "root_path": "",
            "app": app,
        }

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        return Request(scope, receive=receive)

    async def _render_dashboard(
        self,
        *,
        status_filter: str | None = None,
        page: int = 1,
        per_page: int = 20,
    ) -> str:
        request = self._build_request(
            f"page={page}&per_page={per_page}"
            + (f"&status_filter={status_filter}" if status_filter is not None else "")
        )

        async with self.session_factory() as session:
            with (
                patch(
                    "app.routes.admin.resolve_ui_theme",
                    new=AsyncMock(return_value="ocean"),
                ),
                patch.object(
                    admin_routes.redemption_service,
                    "get_stats",
                    new=AsyncMock(return_value={"total": 0, "used": 0}),
                ),
            ):
                response = await admin_routes.admin_dashboard(
                    request=request,
                    page=page,
                    per_page=per_page,
                    search=None,
                    status_filter=status_filter,
                    legacy_status=None,
                    db=session,
                    current_user={"username": "admin", "is_admin": True},
                )

        return bytes(response.body).decode("utf-8")

    async def test_admin_dashboard_defaults_to_normal_filter(self):
        await self._seed_teams()

        body = await self._render_dashboard()

        self.assertRegex(
            body,
            r'id="statusFilterToggleBtn"[\s\S]*?<span>\s*正常\s*</span>',
        )
        self.assertIn("onclick=\"filterByStatus('normal')\"", body)
        self.assertIn("status-badge status-normal", body)
        self.assertIn('name="status_filter" value="normal"', body)
        self.assertIn("Team Active", body)
        self.assertIn("Team Full", body)
        self.assertNotIn("Team Expired", body)

    async def test_admin_dashboard_normal_filter_keeps_query_param_in_pagination(self):
        await self._seed_teams()

        body = await self._render_dashboard(status_filter="normal", per_page=1)

        self.assertIn("status_filter=normal", body)
        self.assertRegex(
            body, r"href=\"\?page=2&amp;status_filter=normal&amp;per_page=1\""
        )

    async def test_admin_dashboard_all_filter_shows_all_statuses(self):
        await self._seed_teams()

        body = await self._render_dashboard(status_filter="all")

        self.assertRegex(
            body,
            r'id="statusFilterToggleBtn"[\s\S]*?<span>\s*所有状态\s*</span>',
        )
        self.assertIn("Team Active", body)
        self.assertIn("Team Full", body)
        self.assertIn("Team Expired", body)


if __name__ == "__main__":
    unittest.main()
