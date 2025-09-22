# from dataclasses import dataclass
# from datetime import datetime
# import typing as t
# from sqlalchemy import orm


# class WorkerDefinition:
#     __tablename__ = "workers"
#     __table_args__ = (
#         orm.Index(
#             "ix_worker_names",
#             "name",
#             unique=True,
#         )
#     )

#     name = orm.mapped_column(orm.String(64), primary_key=True)
#     last_update_time = orm.mapped_column(orm.DateTime(), nullable=False, index=True)
#     last_update_time =  datetime
#     position = orm.mapped_column(orm.Integer)
#   position - current position in all stream
#     last_update_time - last time a worker finished

# class WorkerStates:
#     __tablename__ = "worker_states"
#     __table_args__ = (
#         UniqueConstraint("name"),
#     )

#     id = mapped_column(BigInteger().with_variant(Integer(), "sqlite"), primary_key=True)
#     uuid = mapped_column(GUID(), nullable=False)
#     name = mapped_column(String(255), nullable=True, default=None)
#     category = mapped_column(String(255), nullable=False, default="")
#     tenant_id = mapped_column(String(255), nullable=False)
#     version = mapped_column(BigInteger(), nullable=True)

#     events: Mapped[list["Event"]]
#     snapshots: Mapped[list["Snapshot"]]

#     @hybrid_property
#     def stream_id(self) -> StreamId:
#         return StreamId(self.uuid, self.name, category=self.category or None)

#     @stream_id.inplace.comparator
#     @classmethod
#     def _stream_id_comparator(cls) -> StreamIdComparator:
#         return StreamIdComparator(cls.uuid, cls.name, cls.category)
