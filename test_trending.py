from trending import most_played_beatmaps, most_played_standard_beatmaps


def plays(beatmap_id: int, play_mode: int, play_count: int) -> dict:
    return {
        "beatmap_id": beatmap_id,
        "beatmapset_id": beatmap_id * 10,
        "beatmap_md5": f"{beatmap_id:032x}",
        "song_name": f"Song {beatmap_id}",
        "play_mode": play_mode,
        "play_count": play_count,
    }


def test_most_played_beatmaps_sums_every_table_and_play_mode() -> None:
    vanilla = [plays(1, 0, 5), plays(1, 3, 4), plays(2, 0, 7)]
    relax = [plays(1, 0, 2), plays(3, 1, 9)]
    autopilot = [plays(2, 0, 4)]

    assert most_played_beatmaps(vanilla + relax + autopilot) == [
        {"beatmap_id": 1, "beatmapset_id": 10, "song_name": "Song 1", "play_count": 11},
        {"beatmap_id": 2, "beatmapset_id": 20, "song_name": "Song 2", "play_count": 11},
        {"beatmap_id": 3, "beatmapset_id": 30, "song_name": "Song 3", "play_count": 9},
    ]


def test_most_played_standard_beatmaps_counts_only_osu_standard() -> None:
    vanilla = [plays(1, 0, 5), plays(1, 3, 40), plays(2, 0, 7), plays(3, 0, 5)]

    assert most_played_standard_beatmaps(vanilla) == [
        {
            "beatmap_id": 2,
            "beatmapset_id": 20,
            "beatmap_md5": f"{2:032x}",
            "song_name": "Song 2",
            "play_count": 7,
        },
        {
            "beatmap_id": 1,
            "beatmapset_id": 10,
            "beatmap_md5": f"{1:032x}",
            "song_name": "Song 1",
            "play_count": 5,
        },
        {
            "beatmap_id": 3,
            "beatmapset_id": 30,
            "beatmap_md5": f"{3:032x}",
            "song_name": "Song 3",
            "play_count": 5,
        },
    ]


def test_keeps_the_ten_most_played() -> None:
    rows = [plays(beatmap_id, 0, beatmap_id) for beatmap_id in range(1, 13)]

    for most_played in (most_played_beatmaps, most_played_standard_beatmaps):
        assert [entry["beatmap_id"] for entry in most_played(rows)] == list(
            range(12, 2, -1),
        )
