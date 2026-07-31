import os

from sqlalchemy import Column, Text, create_engine
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import NullPool

Base = declarative_base()


class UploadedID(Base):
    __tablename__ = "uploaded_ids"

    id = Column(Text, primary_key=True)


class HFUploadCheckpoint:
    def __init__(self, branch: str):
        self.path = os.path.join(
            "data",
            "hf_checkpoints",
            f"{branch}.db",
        )
        os.makedirs(os.path.dirname(self.path), exist_ok=True)

        self.engine = create_engine(
            f"sqlite:///{self.path}",
            connect_args={"timeout": 60},
            poolclass=NullPool,
        )
        self.Session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)

    def has_ids(self) -> bool:
        with self.Session() as session:
            return session.query(UploadedID.id).first() is not None

    def existing_ids(self, ids: list[str]) -> set[str]:
        ids = [id_ for id_ in ids if id_]
        if not ids:
            return set()

        found = set()
        with self.Session() as session:
            for start in range(0, len(ids), 900):
                batch = ids[start:start + 900]
                rows = session.query(UploadedID.id).filter(UploadedID.id.in_(batch)).all()
                found.update(row[0] for row in rows)
        return found

    def add_ids(self, ids: list[str]):
        ids = [id_ for id_ in ids if id_]
        if not ids:
            return

        with self.Session() as session:
            session.execute(
                insert(UploadedID)
                .values([{"id": id_} for id_ in ids])
                .prefix_with("OR IGNORE")
            )
            session.commit()
