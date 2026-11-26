"""Most played ranked and approved beatmaps, from per-table play counts.

Each input row is the play count of one beatmap in one play mode within one
scores table, already joined to `beatmaps` and limited to ranked and approved
maps: `beatmap_id`, `beatmapset_id`, `beatmap_md5`, `song_name`, `play_mode`
and `play_count`.
"""

from collections.abc import Iterable, Mapping
from typing import Any

LIMIT = 10
OSU_STANDARD = 0  # play_mode


def most_played_beatmaps(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Plays summed over every row given, whatever its table or play mode."""
    return _most_played(rows, ("beatmap_id", "beatmapset_id", "song_name"))


def most_played_standard_beatmaps(
    rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Plays in osu!standard only, from the rows of one scores table."""
    return _most_played(
        (row for row in rows if row["play_mode"] == OSU_STANDARD),
        ("beatmap_id", "beatmapset_id", "beatmap_md5", "song_name"),
    )


def _most_played(
    rows: Iterable[Mapping[str, Any]],
    fields: tuple[str, ...],
) -> list[dict[str, Any]]:
    """The LIMIT beatmaps with the most plays, most played first, then by beatmap_id.

    Each entry holds `fields` from the beatmap's first row, then `play_count`.
    """
    by_beatmap_id: dict[int, dict[str, Any]] = {}
    for row in rows:
        entry = by_beatmap_id.get(row["beatmap_id"])
        if entry is None:
            entry = {field: row[field] for field in fields}
            entry["play_count"] = 0
            by_beatmap_id[row["beatmap_id"]] = entry
        entry["play_count"] += row["play_count"]

    return sorted(
        by_beatmap_id.values(),
        key=lambda entry: (-entry["play_count"], entry["beatmap_id"]),
    )[:LIMIT]
