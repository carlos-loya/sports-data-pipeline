"""Name normalization using team_aliases.yaml."""

from __future__ import annotations

from functools import lru_cache

import yaml

from sports_pipeline.config import PROJECT_ROOT
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


@lru_cache
def _load_aliases() -> dict[str, dict[str, dict[str, str]]]:
    """Load team alias mappings from YAML."""
    path = PROJECT_ROOT / "config" / "team_aliases.yaml"
    with open(path) as f:
        return yaml.safe_load(f) or {}


class NameNormalizer:
    """Normalize team and player names across data sources."""

    def __init__(self) -> None:
        self._aliases = _load_aliases()
        self._build_lookup()

    def _build_lookup(self) -> None:
        """Build reverse lookup: source_name -> canonical_name."""
        self._lookup: dict[str, dict[str, str]] = {}  # {sport: {alias: canonical}}

        for sport, teams in self._aliases.items():
            self._lookup[sport] = {}
            for canonical, sources in teams.items():
                for source, alias in sources.items():
                    self._lookup[sport][alias.lower()] = canonical
                # Also map canonical to itself
                self._lookup[sport][canonical.lower()] = canonical

    def normalize_team(self, name: str, sport: str) -> str:
        """Normalize a team name to its canonical form.

        Args:
            name: Raw team name from any source
            sport: "soccer" or "basketball"

        Returns:
            Canonical team name, or original if no mapping found.
        """
        if not name:
            return name
        sport_lookup = self._lookup.get(sport, {})
        return sport_lookup.get(name.strip().lower(), name.strip())

    def get_alias(self, canonical_name: str, sport: str, source: str) -> str | None:
        """Get the source-specific alias for a canonical team name."""
        teams = self._aliases.get(sport, {})
        team_aliases = teams.get(canonical_name, {})
        return team_aliases.get(source)

    def get_abbreviation(self, canonical_name: str, sport: str) -> str | None:
        """Get team abbreviation (NBA only)."""
        teams = self._aliases.get(sport, {})
        team_aliases = teams.get(canonical_name, {})
        return team_aliases.get("abbreviation")
