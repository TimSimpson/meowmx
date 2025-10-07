from sqlalchemy import (
    CHAR,
    Column,
    Integer,
    BigInteger,
    Text,
    UniqueConstraint,
    Index,
    ForeignKey,
    PrimaryKeyConstraint,
    text,
)
from sqlalchemy.orm import DeclarativeBase, mapped_column
from sqlalchemy.dialects.postgresql import JSON


class Base(DeclarativeBase):
    pass


class EsAggregate(Base):
    __tablename__ = "es_aggregates"

    id = mapped_column(CHAR(64), primary_key=True)
    version = mapped_column(Integer, nullable=False)
    aggregate_type = mapped_column(Text, nullable=False)

    __table_args__ = (Index("idx_es_aggregate_aggregate_type", "aggregate_type"),)


class EsEvent(Base):
    __tablename__ = "es_events"

    id = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )
    transaction_id = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    aggregate_id = mapped_column(
        CHAR(64),
        ForeignKey("es_aggregate.id", ondelete="CASCADE"),
        nullable=False,
    )
    version = mapped_column(Integer, nullable=False)
    event_type = mapped_column(Text, nullable=False)
    json_data = mapped_column(JSON, nullable=False)

    __table_args__ = (
        UniqueConstraint("aggregate_id", "version", name="uq_es_event_aggid_version"),
        Index(
            "idx_es_event_transaction_id_id",
            "transaction_id",
            "id",
        ),
        Index("idx_es_event_aggregate_id", "aggregate_id"),
        Index("idx_es_event_version", "version"),
    )


class EsAggregateSnapshot(Base):
    __tablename__ = "es_aggregate_snapshots"

    aggregate_id = mapped_column(
        CHAR(64),
        ForeignKey("es_aggregate.id", ondelete="CASCADE"),
        nullable=False,
    )
    version = Column(Integer, nullable=False)
    json_data = Column(JSON, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint(
            "aggregate_id", "version", name="pk_es_aggregate_snapshot"
        ),
    )


class EsEventSubscription(Base):
    __tablename__ = "es_event_subscriptions"

    subscription_name = mapped_column(Text, primary_key=True)
    last_transaction_id = mapped_column(BigInteger, nullable=False)
    last_event_id = mapped_column(BigInteger, nullable=False)
