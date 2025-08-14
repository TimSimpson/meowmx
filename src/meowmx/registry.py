import typing as t
from event_sourcery.event_store import EventRegistry
import pydantic

TEvent: t.TypeAlias = pydantic.BaseModel


class Unknown(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(
        extra="allow",
    )


class LenientEventRegistry(EventRegistry):
    """
    Like the normal event registry, but if the event type hasn't been seen
    it returns Unknown.
    """

    def type_for_name(self, name: str) -> t.Type[TEvent]:
        try:
            return super().type_for_name(name)
        except KeyError:
            return Unknown

    def encrypted_fields(self, *args, **kwargs) -> t.Any:
        try:
            return super().encrypted_fields(*args, **kwargs)
        except KeyError:
            return {}
