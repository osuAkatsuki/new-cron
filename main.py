#!/usr/bin/env python3.9
from cmyui.mysql import AsyncSQLPool
from cmyui.discord import Webhook, Embed
from aiohttp import ClientSession

import aioredis
import asyncio
import config
import uvloop
import time

db = AsyncSQLPool()
redis: "aioredis.Redis"


async def connect() -> None:
    await db.connect(config.sql)

    global redis
    redis = await aioredis.create_redis_pool("redis://localhost:6379/0")

    print("Connected to database and redis")


async def disconnect() -> None:
    await db.close()

    redis.close()
    await redis.wait_closed()

    print("Disconnected from database and redis")


async def recalc_ranks() -> None:
    print("Recalculating all user ranks")

    current_time = int(time.time())
    for rx in (
        0,
        1,
    ):
        stats_table = "rx_stats" if rx else "users_stats"
        redis_board = "relaxboard" if rx else "leaderboard"
        for mode in ("std", "taiko", "ctb", "mania"):
            users = await db.fetchall(
                f"select stats.id, stats.pp_{mode} pp, "
                "stats.country, users.latest_activity, users.privileges "
                f"from {stats_table} stats "
                "left join users on stats.id = users.id "
                f"where stats.pp_{mode} > 0"
            )

            for user in users:
                if not all((user["privileges"], user["latest_activity"])):
                    await redis.zrem(f"ripple:{redis_board}:{mode}", user["id"])
                    await redis.zrem(
                        f"ripple:{redis_board}:{mode}:{user['country']}", user["id"]
                    )
                    continue

                if not int(user["privileges"]) & 1:
                    await redis.zrem(f"ripple:{redis_board}:{mode}", user["id"])
                    await redis.zrem(
                        f"ripple:{redis_board}:{mode}:{user['country']}", user["id"]
                    )
                    continue

                inactive_days = (current_time - user["latest_activity"]) / 60 / 60 / 24
                if inactive_days > 60:
                    await redis.zrem(f"ripple:{redis_board}:{mode}", user["id"])
                    await redis.zrem(
                        f"ripple:{redis_board}:{mode}:{user['country']}", user["id"]
                    )
                    continue

                await redis.zadd(f"ripple:{redis_board}:{mode}", user["pp"], user["id"])

                country = user["country"].lower()
                if country != "xx":
                    await redis.zadd(
                        f"ripple:{redis_board}:{mode}:{country}", user["pp"], user["id"]
                    )

    print(f"Recalculated all ranks in {time.time() - current_time:.2f} seconds")


async def fix_supporter_badges() -> None:
    print("Fixing all supporter badges")

    current_time = int(time.time())
    expired_donors = await db.fetchall(
        "select id, privileges from users where privileges & 4 and donor_expire < %s",
        (current_time,),
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
        (current_time,),
    )

    await db.execute(
        "update users_stats left join users using(id) set users_stats.can_custom_badge = 0 where users.donor_expire < %s",
        (current_time,),
    )

    # now fix missing custom badges
    await db.execute(
        "update users_stats left join users using(id) set users_stats.can_custom_badge = 1 where users.donor_expire > %s",
        (current_time,),
    )

    print(f"Fixed all supporter badges in {time.time() - current_time:.2f} seconds")


FREEZE_MESSAGE = "has been automatically restricted due to a pending freeze."


async def fix_freeze_timers() -> None:
    print("Fixing expired freeze timers")

    expired_users = await db.fetchall(
        "select id, username, privileges, frozen from users where frozen != 0 and frozen != 1"
    )

    current_time = int(time.time())
    for user in expired_users:
        new_priv = user["privileges"] & ~1

        if int(user["frozen"]) != 0 and user["frozen"] > current_time:
            continue

        await db.execute(
            "update users set privileges = %s, frozen = 0, ban_datetime = UNIX_TIMESTAMP() where id = %s",
            (
                new_priv,
                user["id"],
            ),
        )

        await redis.publish("peppy:ban", user["id"])

        for board in ("leaderboard", "relaxboard"):
            await redis.zrem(f"ripple:{board}:*:*", user["id"])

        await db.execute(
            "insert into rap_logs (id, userid, text, datetime, through) values (null, %s, %s, UNIX_TIMESTAMP(), %s)",
            (user["id"], FREEZE_MESSAGE, "Aika"),
        )

        # post to webhook
        webhook = Webhook(config.ac_webhook)
        embed = Embed(color=0x542CB8)

        embed.add_field(name="** **", value=f"{user['username']} {FREEZE_MESSAGE}")
        embed.set_footer(text="Akatsuki Anticheat")
        embed.set_thumbnail(url="https://akatsuki.pw/static/logos/logo.png")

        webhook.add_embed(embed)

        async with ClientSession() as session:
            await webhook.post(session)

    print(f"Fixed all freeze timers in {time.time() - current_time:.2f} seconds")

async def clear_scores() -> None:
    print("Deleting clearable scores")

    current_time = int(time.time())

    for table in ("scores", "scores_relax"):
        await db.execute(
            f"delete from {table} where completed < 3 and time < UNIX_TIMESTAMP(NOW() - INTERVAL 24 HOUR)"
        )

    print(f"Deleted all clearable scores in {time.time() - current_time:.2f} seconds")

async def main() -> None:
    print("Starting Akatsuki cron")

    current_time = int(time.time())

    await connect()

    await recalc_ranks()
    await fix_supporter_badges()
    await fix_freeze_timers()
    await clear_scores()

    await disconnect()

    print(f"Finished running cron in {time.time() - current_time:.2f} seconds")


uvloop.install()
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
