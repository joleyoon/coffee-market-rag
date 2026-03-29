"""Shared helpers for dataset versioning and metadata enrichment."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
import shutil

from scripts.report_utils import ensure_directory, load_json


DEFAULT_PROCESSED_ROOT = Path("data/processed/ico")
DEFAULT_VERSIONS_ROOT = DEFAULT_PROCESSED_ROOT / "versions"
DEFAULT_PIPELINE_MANIFEST = DEFAULT_PROCESSED_ROOT / "pipeline_manifest.json"
DEFAULT_STATIC_SEARCH_DATA = Path("docs/data/search-data.json")

COUNTRY_ALIASES = {
    "Brazil": ("brazil", "brazilian"),
    "Vietnam": ("vietnam", "vietnamese"),
    "Colombia": ("colombia", "colombian"),
    "Honduras": ("honduras", "honduran"),
    "India": ("india", "indian"),
    "Uganda": ("uganda", "ugandan"),
    "Ethiopia": ("ethiopia", "ethiopian"),
    "Indonesia": ("indonesia", "indonesian"),
    "Peru": ("peru", "peruvian"),
    "Mexico": ("mexico", "mexican"),
    "Guatemala": ("guatemala", "guatemalan"),
    "Nicaragua": ("nicaragua", "nicaraguan"),
    "Costa Rica": ("costa rica", "costa rican"),
    "El Salvador": ("el salvador", "salvadoran"),
    "Kenya": ("kenya", "kenyan"),
    "Tanzania": ("tanzania", "tanzanian"),
    "Rwanda": ("rwanda", "rwandan"),
    "Burundi": ("burundi", "burundian"),
    "Cameroon": ("cameroon", "cameroonian"),
    "Cote d'Ivoire": ("cote d'ivoire", "cote d ivoire", "ivory coast", "ivoirian"),
    "Laos": ("laos", "laotian"),
    "China": ("china", "chinese"),
    "Papua New Guinea": ("papua new guinea",),
    "Ecuador": ("ecuador", "ecuadorean", "ecuadorian"),
    "Venezuela": ("venezuela", "venezuelan"),
    "United States": ("united states", "usa", "u.s."),
    "Japan": ("japan", "japanese"),
    "Germany": ("germany", "german"),
    "Italy": ("italy", "italian"),
    "Belgium": ("belgium", "belgian"),
    "South Korea": ("south korea", "korea", "korean"),
}

COFFEE_TYPE_PATTERNS = {
    "Arabica": (
        r"\barabica(?:s)?\b",
        r"\bcolombian milds?\b",
        r"\bother milds?\b",
        r"\bbrazilian naturals?\b",
        r"\bmild coffees?\b",
    ),
    "Robusta": (
        r"\brobusta(?:s)?\b",
        r"\brobusta coffees?\b",
    ),
}


@dataclass(frozen=True)
class VersionPaths:
    """Filesystem layout for a single dataset snapshot."""

    dataset_version: str
    root: Path

    @property
    def extracted_dir(self) -> Path:
        return self.root / "extracted_text"

    @property
    def extracted_jsonl(self) -> Path:
        return self.extracted_dir / "reports.jsonl"

    @property
    def chunks_path(self) -> Path:
        return self.root / "chunks" / "chunks.jsonl"

    @property
    def index_path(self) -> Path:
        return self.root / "index" / "tfidf_index.pkl"

    @property
    def trend_path(self) -> Path:
        return self.root / "trends" / "trend-data.json"

    @property
    def pipeline_manifest_path(self) -> Path:
        return self.root / "pipeline_manifest.json"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_timestamp(value: datetime | None = None) -> str:
    current = value or utc_now()
    return current.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_dataset_version(value: datetime | None = None) -> str:
    current = value or utc_now()
    return current.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def build_version_paths(dataset_version: str, versions_root: Path = DEFAULT_VERSIONS_ROOT) -> VersionPaths:
    return VersionPaths(dataset_version=dataset_version, root=versions_root / dataset_version)


def relative_path(path: Path) -> str:
    return str(path).replace("\\", "/")


def latest_manifest(path: Path = DEFAULT_PIPELINE_MANIFEST) -> dict | None:
    if not path.exists():
        return None
    return load_json(path)


def extract_country_tags(text: str) -> list[str]:
    lowered = text.lower()
    tags: list[str] = []
    for country, aliases in COUNTRY_ALIASES.items():
        if any(re.search(rf"\b{re.escape(alias)}\b", lowered) for alias in aliases):
            tags.append(country)
    return tags


def extract_coffee_type_tags(text: str) -> list[str]:
    tags: list[str] = []
    for coffee_type, patterns in COFFEE_TYPE_PATTERNS.items():
        if any(re.search(pattern, text, re.IGNORECASE) for pattern in patterns):
            tags.append(coffee_type)
    return tags


def build_date_metadata(published_date: str | None) -> dict:
    if not published_date:
        return {
            "published_date": None,
            "report_month": None,
            "report_year": None,
        }
    return {
        "published_date": published_date,
        "report_month": published_date[:7],
        "report_year": published_date[:4],
    }


def classify_report_period(published_date: str | None, latest_published_date: str | None) -> str:
    if not published_date or not latest_published_date:
        return "historical"
    if published_date == latest_published_date:
        return "latest"
    return "historical"


def sync_directory(source_dir: Path, destination_dir: Path, pattern: str) -> None:
    ensure_directory(destination_dir)
    source_files = {path.name: path for path in source_dir.glob(pattern)}
    destination_files = {path.name: path for path in destination_dir.glob(pattern)}

    for stale_name in sorted(destination_files.keys() - source_files.keys()):
        destination_files[stale_name].unlink()

    for file_name, source_path in source_files.items():
        shutil.copy2(source_path, destination_dir / file_name)


def publish_latest_aliases(version_paths: VersionPaths, processed_root: Path = DEFAULT_PROCESSED_ROOT) -> None:
    ensure_directory(processed_root / "index")
    ensure_directory(processed_root / "chunks")
    ensure_directory(processed_root / "trends")

    shutil.copy2(version_paths.index_path, processed_root / "index" / "tfidf_index.pkl")
    shutil.copy2(version_paths.chunks_path, processed_root / "chunks" / "chunks.jsonl")
    shutil.copy2(version_paths.pipeline_manifest_path, processed_root / "pipeline_manifest.json")

    if version_paths.trend_path.exists():
        shutil.copy2(version_paths.trend_path, processed_root / "trends" / "trend-data.json")

    sync_directory(version_paths.extracted_dir, processed_root / "extracted_text", "*.json")
    shutil.copy2(version_paths.extracted_jsonl, processed_root / "extracted_text" / "reports.jsonl")
