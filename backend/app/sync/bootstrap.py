"""Historical bootstrap CLI: `python -m app.sync.bootstrap <basho_id>...`."""
from __future__ import annotations

import argparse
import asyncio
import logging

from app.db.session import SessionLocal
from app.sync.sumo_api import SumoApiClient, sync_full_basho


async def _main(basho_ids: list[str]) -> None:
    async with SumoApiClient() as api:
        async with SessionLocal() as session:
            for basho_id in basho_ids:
                report = await sync_full_basho(session, api, basho_id)
                logging.info("synced %s: +%d matches", basho_id, report.new_matches)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("basho_ids", nargs="+", help="e.g. 202405 202403")
    args = parser.parse_args()
    asyncio.run(_main(args.basho_ids))


if __name__ == "__main__":
    main()
