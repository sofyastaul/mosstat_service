import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

def _bool(v: str, default: bool=False) -> bool:
    if v is None:
        return default
    return str(v).strip().lower() in {"1","true","yes","y","on"}

@dataclass(frozen=True)
class Settings:
    pg_host: str = os.getenv("PG_HOST", "localhost")
    pg_port: int = int(os.getenv("PG_PORT", "5432"))
    pg_db: str = os.getenv("PG_DB", "practice")
    pg_user: str = os.getenv("PG_USER", "postgres")
    pg_password: str = os.getenv("PG_PASSWORD", "")
    verify_ssl: bool = _bool(os.getenv("VERIFY_SSL", "false"), default=False)

settings = Settings()
