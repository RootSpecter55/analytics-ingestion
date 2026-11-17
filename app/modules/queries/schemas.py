import re
from datetime import date
from typing import Self

from pydantic import Field, field_validator, model_validator

from app.core.schemas import ApiModel
from app.modules.event_schemas.schemas import EVENT_NAME_PATTERN

MAX_RANGE_DAYS = 366
_EVENT_NAME = re.compile(EVENT_NAME_PATTERN)


def check_range(start: date, end: date, label: str = "from/to") -> None:
    if end < start:
        raise ValueError(f"{label}: the end is before the start")
    if (end - start).days >= MAX_RANGE_DAYS:
        raise ValueError(f"{label}: at most {MAX_RANGE_DAYS} days")


class FunnelRequest(ApiModel):
    steps: list[str] = Field(min_length=2, max_length=5)
    from_: date = Field(alias="from")
    to: date
    # How long after the first step a user may take to finish the funnel.
    window_seconds: int = Field(ge=1, le=90 * 24 * 3600)

    @field_validator("steps")
    @classmethod
    def check_names(cls, steps: list[str]) -> list[str]:
        for step in steps:
            if not _EVENT_NAME.fullmatch(step):
                raise ValueError(f"not a valid event name: {step!r}")
        return steps

    @model_validator(mode="after")
    def check_dates(self) -> Self:
        check_range(self.from_, self.to)
        return self


class RetentionRequest(ApiModel):
    start_event: str = Field(pattern=EVENT_NAME_PATTERN)
    return_event: str = Field(pattern=EVENT_NAME_PATTERN)
    cohort_from: date
    cohort_to: date
    # Day offsets to report, e.g. [1, 7, 30] for day-1, day-7 and day-30 retention.
    days: list[int] = Field(min_length=1, max_length=10)

    @field_validator("days")
    @classmethod
    def check_days(cls, days: list[int]) -> list[int]:
        if any(not 1 <= day <= 90 for day in days):
            raise ValueError("each day must be between 1 and 90")
        return sorted(set(days))

    @model_validator(mode="after")
    def check_dates(self) -> Self:
        check_range(self.cohort_from, self.cohort_to, "cohortFrom/cohortTo")
        return self


class DailyCountOut(ApiModel):
    date: date
    events: int


class UniqueUsersOut(ApiModel):
    from_: date = Field(alias="from")
    to: date
    event: str | None
    users: int


class FunnelStepOut(ApiModel):
    step: int
    name: str
    users: int
    # Share of the users who did step 1 that got this far.
    conversion: float


class RetentionPointOut(ApiModel):
    day: int
    cohort_size: int
    retained: int
    rate: float
