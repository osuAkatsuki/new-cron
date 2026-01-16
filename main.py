#!/usr/bin/env python3.9
import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Any, cast

import redis.asyncio as aioredis
import uvloop
from aiohttp import ClientSession
from cmyui.discord import Embed, Webhook
from cmyui.mysql import AsyncSQLPool

import settings

db = AsyncSQLPool()
redis = aioredis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    username=settings.REDIS_USER,
    password=settings.REDIS_PASS,
    db=settings.REDIS_DB,
    ssl=settings.REDIS_USE_SSL,
)


def unix_to_iso(timestamp: int) -> str:
    """Convert Unix timestamp to ISO 8601 format string."""
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def convert_timestamps_to_iso(data: list[dict]) -> list[dict]:
    """Convert Unix timestamp fields to ISO format in a list of dictionaries."""
    for item in data:
        # Convert score_time (for first places)
        if "score_time" in item and isinstance(item["score_time"], int):
            item["score_time"] = unix_to_iso(item["score_time"])
        # Convert time (for high PP plays)
        if "time" in item and isinstance(item["time"], int):
            item["time"] = unix_to_iso(item["time"])
    return data


async def connect() -> None:
    await db.connect(
        {
            "db": settings.DB_NAME,
            "host": settings.DB_HOST,
            "password": settings.DB_PASS,
            "user": settings.DB_USER,
            "port": settings.DB_PORT,
        },
    )

    await redis.initialize()  # type: ignore[unused-awaitable]
    await redis.ping()

    print("Connected to database and redis")


async def disconnect() -> None:
    await db.close()

    await redis.aclose()

    print("Disconnected from database and redis")


STR_TO_INT_MODE = {"std": 0, "taiko": 1, "ctb": 2, "mania": 3}


REDIS_BATCH_SIZE = 1000


async def recalc_ranks() -> None:
    print("Recalculating all user ranks")

    start_time = int(time.time())
    for rx in (0, 1, 2):
        if rx == 0:
            redis_board = "leaderboard"
            modes = ("std", "taiko", "ctb", "mania")
        elif rx == 1:
            redis_board = "relaxboard"
            modes = ("std", "taiko", "ctb")
        else:  # rx == 2:
            redis_board = "autoboard"
            modes = ("std",)

        for mode in modes:
            users = await db.fetchall(
                """
                SELECT users.id, user_stats.pp, user_stats.latest_pp_awarded,
                users.country, users.latest_activity, users.privileges
                FROM users
                LEFT JOIN user_stats on user_stats.user_id = users.id
                WHERE user_stats.pp > 0
                AND mode = %s
                """,
                (STR_TO_INT_MODE[mode] + (rx * 4),),
            )
            users = cast(list[dict[str, Any]], users)

            rank_key = f"ripple:{redis_board}:{mode}"

            for i in range(0, len(users), REDIS_BATCH_SIZE):
                batch = users[i : i + REDIS_BATCH_SIZE]
                async with redis.pipeline() as pipe:
                    for user in batch:
                        inactive_days = (
                            (start_time - user["latest_pp_awarded"]) / 60 / 60 / 24
                        )
                        country = user["country"].lower()

                        user_country_rank_key = f"{rank_key}:{country}"
                        if (
                            inactive_days < 60
                            and user["privileges"] & 1
                            and user["pp"] > 0
                        ):
                            await pipe.zadd(
                                rank_key,
                                {user["id"]: user["pp"]},
                            )

                            if country != "xx":
                                await pipe.zadd(
                                    user_country_rank_key,
                                    {user["id"]: user["pp"]},
                                )
                        else:
                            await pipe.zrem(rank_key, user["id"])
                            await pipe.zrem(user_country_rank_key, user["id"])

                    await pipe.execute()

    print(f"Recalculated all ranks in {time.time() - start_time:.2f} seconds")


async def fix_supporter_badges() -> None:
    print("Fixing all supporter badges")

    start_time = int(time.time())
    expired_donors = await db.fetchall(
        "select id, privileges from users where privileges & 4 and donor_expire < %s",
        (start_time,),
    )

    for user in expired_donors:
        premium = user["privileges"] & 8388608

        await db.execute(
            "update users set privileges = privileges - %s where id = %s",
            (
                8388612 if premium else 4,
                user["id"],
            ),
        )

        await db.execute(
            "delete from user_badges where badge in (59, 36) and user = %s",
            (user["id"],),
        )

    # wipe any somehow missed badges
    await db.execute(
        "delete user_badges from user_badges left join users on user_badges.user = users.id where badge in (59, 36) and users.donor_expire < %s",
        (start_time,),
    )

    # remove custom badge perms from any expired donors
    await db.execute(
        "update users set can_custom_badge = 0 where donor_expire < %s",
        (start_time,),
    )

    # now fix missing custom badges
    await db.execute(
        "update users set can_custom_badge = 1 where donor_expire > %s",
        (start_time,),
    )

    print(f"Fixed all supporter badges in {time.time() - start_time:.2f} seconds")


async def update_total_submitted_score_counts() -> None:
    print("Updating total submitted score counts")

    start_time = time.time()

    # scores
    row = await db.fetch(
        """
        SELECT AUTO_INCREMENT
          FROM INFORMATION_SCHEMA.TABLES
         WHERE TABLE_SCHEMA = 'akatsuki'
           AND TABLE_NAME = 'scores'
      ORDER BY AUTO_INCREMENT DESC
        """,
    )
    if row is None:
        raise Exception("Couldn't fetch auto_increment for scores")

    await redis.set(
        "ripple:submitted_scores",
        str(row["AUTO_INCREMENT"] - 500_000_000),
    )

    # scores_relax
    row = await db.fetch(
        """
        SELECT AUTO_INCREMENT
          FROM INFORMATION_SCHEMA.TABLES
         WHERE TABLE_SCHEMA = 'akatsuki'
           AND TABLE_NAME = 'scores_relax'
      ORDER BY AUTO_INCREMENT DESC
        """,
    )
    if row is None:
        raise Exception("Couldn't fetch auto_increment for scores_relax")

    await redis.set(
        "ripple:submitted_scores_relax",
        str(row["AUTO_INCREMENT"]),
    )

    # scores_ap
    row = await db.fetch(
        """
        SELECT AUTO_INCREMENT
          FROM INFORMATION_SCHEMA.TABLES
         WHERE TABLE_SCHEMA = 'akatsuki'
           AND TABLE_NAME = 'scores_ap'
      ORDER BY AUTO_INCREMENT DESC
        """,
    )
    if row is None:
        raise Exception("Couldn't fetch auto_increment for scores_ap")

    await redis.set(
        "ripple:submitted_scores_ap",
        str(row["AUTO_INCREMENT"] - 6_148_914_691_236_517_204),
    )

    print(
        f"Updated total submitted score counts in {time.time() - start_time:.2f} seconds",
    )


FREEZE_MESSAGE = "has been automatically restricted due to a pending freeze."


BOARD_MODES = {
    "leaderboard": ("std", "taiko", "ctb", "mania"),
    "relaxboard": ("std", "taiko", "ctb"),
    "autoboard": ("std",),
}


async def freeze_expired_freeze_timers() -> None:
    print("Freezing users with expired freeze timers")

    expired_users = await db.fetchall(
        "select id, username, privileges, frozen, country from users where frozen != 0 and frozen != 1",
    )

    start_time = int(time.time())
    for user in expired_users:
        new_priv = user["privileges"] & ~1

        if int(user["frozen"]) != 0 and user["frozen"] > start_time:
            continue

        await db.execute(
            "update users set privileges = %s, frozen = 0, ban_datetime = UNIX_TIMESTAMP() where id = %s",
            (
                new_priv,
                user["id"],
            ),
        )

        await redis.publish("peppy:ban", user["id"])

        country = user["country"].lower()
        async with redis.pipeline() as pipe:
            for board, modes in BOARD_MODES.items():
                for mode in modes:
                    await pipe.zrem(f"ripple:{board}:{mode}", user["id"])
                    if country != "xx":
                        await pipe.zrem(f"ripple:{board}:{mode}:{country}", user["id"])
            await pipe.execute()

        await db.execute(
            "insert into rap_logs (id, userid, text, datetime, through) values (null, %s, %s, UNIX_TIMESTAMP(), %s)",
            (user["id"], FREEZE_MESSAGE, "Aika"),
        )

        # post to webhook
        webhook = Webhook(settings.DISCORD_AC_WEBHOOK)
        embed = Embed(color=0x542CB8)

        embed.add_field(name="** **", value=f"{user['username']} {FREEZE_MESSAGE}")
        embed.set_footer(text="Akatsuki Anticheat")
        embed.set_thumbnail(url="https://akatsuki.pw/static/logos/logo.png")

        webhook.add_embed(embed)

        async with ClientSession() as session:
            await webhook.post(session)

    print(f"Froze all users in {time.time() - start_time:.2f} seconds")


async def update_hanayo_country_list() -> None:
    COUNTRY_LIST_KEY = "hanayo:country_list"

    print("Updating hanayo country list")
    start_time = int(time.time())

    country_list = await db.fetchall(
        """
        SELECT country, COUNT(*) AS user_count
          FROM users
          INNER JOIN user_stats ON user_stats.user_id = id
         WHERE LENGTH(country) = 2
            AND country != 'XX'
            AND PRIVILEGES & 1
            AND latest_pp_awarded > (UNIX_TIMESTAMP() - 60 * 24 * 60 * 60)
        GROUP BY country
        """,
    )
    async with redis.pipeline() as pipe:
        await pipe.delete(COUNTRY_LIST_KEY)
        await pipe.zadd(
            COUNTRY_LIST_KEY,
            {country["country"]: country["user_count"] for country in country_list},
        )

        await pipe.execute()

    print(f"Updated hanayo country list in {time.time() - start_time:.2f} seconds")


async def update_top_plays() -> None:
    for table, whitelist_int in (
        ("scores", 1),
        ("scores_relax", 2),
        ("scores_ap", 2),
    ):
        query = f"""
            SELECT users.username,
                   users.id,
                   ROUND(s.pp) AS pp
              FROM {table} s
         LEFT JOIN beatmaps b
             USING (beatmap_md5)
         LEFT JOIN users ON users.id = s.userid
             WHERE s.play_mode = 0
               AND s.completed = 3
               AND b.ranked = 2
               AND users.privileges & 1
               AND users.whitelist & {whitelist_int}
          ORDER BY s.pp DESC
             LIMIT 1
            """

        result = await db.fetch(query)
        async with redis.pipeline() as pipe:
            if not result:
                await redis.set(f"akatsuki:top:{table}:pp", 0)
                await redis.set(f"akatsuki:top:{table}:id", 0)
                await redis.set(f"akatsuki:top:{table}:name", "")
            else:
                await redis.set(f"akatsuki:top:{table}:pp", int(result["pp"]))
                await redis.set(f"akatsuki:top:{table}:id", result["id"])
                await redis.set(f"akatsuki:top:{table}:name", result["username"])

            await pipe.execute()

    print(f"Updated top play cache!")


async def update_homepage_cache() -> None:
    """Update cached data for the homepage activity feed and stats."""
    print("Updating homepage cache")
    start_time = time.time()

    # Mode configurations:
    # Combined mode = base_mode + (rx * 4), except ap which is always 8
    # Vanilla (rx=0): modes 0-3 (std, taiko, ctb, mania)
    # Relax (rx=1): modes 4-6 (std, taiko, ctb - no mania)
    # Autopilot (rx=2): mode 8 (std only)
    # PP thresholds for high PP plays display
    mode_configs = [
        # (combined_mode, rx, play_mode, scores_table, pp_threshold)
        (0, 0, 0, "scores", 700),  # vn std
        (1, 0, 1, "scores", 700),  # vn taiko
        (2, 0, 2, "scores", 700),  # vn ctb
        (3, 0, 3, "scores", 600),  # vn mania
        (4, 1, 0, "scores_relax", 1300),  # rx std
        (5, 1, 1, "scores_relax", 800),  # rx taiko
        (6, 1, 2, "scores_relax", 700),  # rx ctb
        (8, 2, 0, "scores_ap", 500),  # ap std
    ]

    for combined_mode, rx, play_mode, scores_table, pp_threshold in mode_configs:
        # Recent first places for this mode
        first_places = await db.fetchall(
            f"""
            SELECT sf.scoreid, sf.userid, u.username, u.country,
                   b.song_name, b.beatmap_id, b.beatmapset_id,
                   ROUND(s.pp) as pp, s.time as score_time,
                   s.mods, s.accuracy
            FROM scores_first sf
            INNER JOIN users u ON u.id = sf.userid
            INNER JOIN beatmaps b ON b.beatmap_md5 = sf.beatmap_md5
            INNER JOIN {scores_table} s ON s.id = sf.scoreid
            WHERE sf.rx = %s AND sf.mode = %s AND u.privileges & 1
            ORDER BY s.time DESC
            LIMIT 10
            """,
            (rx, play_mode),
        )
        # Convert Unix timestamps to ISO format
        first_places = convert_timestamps_to_iso(first_places)
        await redis.set(
            f"akatsuki:first_places:{combined_mode}",
            json.dumps(first_places, default=str),
        )

        # High PP plays for this mode (last 24h)
        high_pp = await db.fetchall(
            f"""
            SELECT s.id, s.userid, ROUND(s.pp) as pp, s.time,
                   u.username, u.country,
                   b.song_name, b.beatmap_id, b.beatmapset_id,
                   s.mods, s.accuracy
            FROM {scores_table} s
            INNER JOIN users u ON u.id = s.userid
            INNER JOIN beatmaps b ON b.beatmap_md5 = s.beatmap_md5
            WHERE s.pp >= %s AND s.completed = 3 AND s.play_mode = %s
              AND s.time > UNIX_TIMESTAMP() - 86400
              AND u.privileges & 1 AND b.ranked IN (2, 3)
            ORDER BY s.time DESC
            LIMIT 10
            """,
            (pp_threshold, play_mode),
        )
        # Convert Unix timestamps to ISO format
        high_pp = convert_timestamps_to_iso(high_pp)
        await redis.set(
            f"akatsuki:high_pp:{combined_mode}",
            json.dumps(high_pp, default=str),
        )

    # Default keys (vn std - mode 0) for initial page load via templates
    vn_std_first_places = await redis.get("akatsuki:first_places:0")
    vn_std_high_pp = await redis.get("akatsuki:high_pp:0")
    if vn_std_first_places:
        await redis.set("akatsuki:recent_first_places", vn_std_first_places)
    if vn_std_high_pp:
        await redis.set("akatsuki:high_pp_plays_24h", vn_std_high_pp)

    # Trending beatmaps (most played this week)
    trending = await db.fetchall(
        """
        SELECT b.beatmap_id, b.beatmapset_id, b.song_name, COUNT(*) as play_count
        FROM (
            SELECT beatmap_md5 FROM scores WHERE time > UNIX_TIMESTAMP() - 604800
            UNION ALL
            SELECT beatmap_md5 FROM scores_relax WHERE time > UNIX_TIMESTAMP() - 604800
            UNION ALL
            SELECT beatmap_md5 FROM scores_ap WHERE time > UNIX_TIMESTAMP() - 604800
        ) recent
        INNER JOIN beatmaps b ON b.beatmap_md5 = recent.beatmap_md5
        WHERE b.ranked IN (2, 3)
        GROUP BY b.beatmap_id
        ORDER BY play_count DESC
        LIMIT 10
        """,
    )
    await redis.set(
        "akatsuki:trending_beatmaps",
        json.dumps(trending, default=str),
    )

    # Simple counts
    user_count = await db.fetch(
        """
        SELECT COUNT(*) AS cnt
        FROM users
        WHERE privileges & 1
        """
    )
    await redis.set("akatsuki:registered_users", str(user_count["cnt"]))

    beatmap_count = await db.fetch(
        """
        SELECT COUNT(*) AS cnt
        FROM beatmaps
        WHERE ranked IN (2, 3)
        """
    )
    await redis.set("akatsuki:ranked_beatmaps", str(beatmap_count["cnt"]))

    playtime = await db.fetch(
        """
        SELECT SUM(playtime) AS total_playtime
        FROM user_stats
        INNER JOIN users ON users.id = user_stats.user_id
        WHERE users.privileges & 1
        """
    )
    years = int(playtime["total_playtime"]) // (60 * 60 * 24 * 365)
    await redis.set("akatsuki:total_playtime_years", str(years))

    # New registrations (last 24h)
    new_users = await db.fetch(
        """
        SELECT COUNT(*) AS cnt
        FROM users
        WHERE privileges & 1
        AND register_datetime > UNIX_TIMESTAMP() - 86400
        """,
    )
    await redis.set(
        "akatsuki:new_registrations_24h",
        str(new_users["cnt"]),
    )

    print(f"Updated homepage cache in {time.time() - start_time:.2f} seconds")


async def main() -> None:
    print("Starting Akatsuki cron")

    start_time = int(time.time())

    await connect()

    await recalc_ranks()
    await update_hanayo_country_list()
    await fix_supporter_badges()
    await update_total_submitted_score_counts()
    await freeze_expired_freeze_timers()
    await update_top_plays()
    await update_homepage_cache()

    # NOTE: this cron used to delete scores over
    # 24 hours old with completed < 3, but it was
    # disabled as of 2022-07-19.

    await disconnect()

    print(f"Finished running cron in {time.time() - start_time:.2f} seconds")


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
