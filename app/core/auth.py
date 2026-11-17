from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Annotated, Literal
from uuid import UUID

from fastapi import Header

from app.core.errors import UnauthorizedError
from app.core.security import bearer_token, keys_match


@dataclass(frozen=True)
class ProjectContext:
    """The project a request acts for. It always comes from the key, never from the request."""

    project_id: UUID
    name: str


KeyKind = Literal["write", "read"]
ResolveProject = Callable[[str, KeyKind], Awaitable[ProjectContext | None]]


def make_admin_auth(admin_key: str) -> Callable[..., Awaitable[None]]:
    """FastAPI dependency: only `Authorization: Bearer <ADMIN_API_KEY>` gets through."""

    async def require_admin(authorization: Annotated[str | None, Header()] = None) -> None:
        token = bearer_token(authorization)
        if not token or not keys_match(token, admin_key):
            raise UnauthorizedError("Invalid admin key")

    return require_admin


def make_project_auth(
    resolve: ResolveProject, kind: KeyKind
) -> Callable[..., Awaitable[ProjectContext]]:
    """FastAPI dependency: the project whose write (or read) key is in the bearer header. A read
    key cannot ingest and a write key cannot query. `resolve` is supplied by the projects
    module, so this file does not depend on any module."""

    async def current_project(
        authorization: Annotated[str | None, Header()] = None,
    ) -> ProjectContext:
        token = bearer_token(authorization)
        project = await resolve(token, kind) if token else None
        if project is None:
            raise UnauthorizedError(f"Invalid {kind} key")
        return project

    return current_project
