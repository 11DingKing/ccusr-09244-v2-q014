import os
import tempfile

# 必须在任何 app 模块导入之前指定测试数据库，避免触碰本地开发库。
_TMP_DIR = tempfile.mkdtemp(prefix="recalc_test_")
os.environ["DATABASE_URL"] = f"sqlite:///{_TMP_DIR}/app.db"

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base


@pytest.fixture()
def session_factory(tmp_path):
    """每个测试一个独立的 SQLite 文件库，返回会话工厂。"""
    engine = create_engine(
        f"sqlite:///{tmp_path}/test.db",
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    try:
        yield factory
    finally:
        engine.dispose()


@pytest.fixture()
def db(session_factory):
    session = session_factory()
    try:
        yield session
    finally:
        session.close()
