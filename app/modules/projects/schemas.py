from datetime import datetime
from uuid import UUID

from pydantic import Field

from app.core.schemas import ApiModel


class CreateProjectRequest(ApiModel):
    name: str = Field(min_length=1, max_length=100)


class ProjectOut(ApiModel):
    id: UUID
    name: str
    created_at: datetime


class CreatedProject(ApiModel):
    project: ProjectOut
    write_key: str
    read_key: str
