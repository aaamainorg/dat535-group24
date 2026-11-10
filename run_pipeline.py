#!/usr/bin/env python3
"""
DAT535 Group 24 - pipeline orchestrator
========================================

One entry point for every layer, used both on the server and by GitHub Actions.

Usage:
    python run_pipeline.py bronze      # ingest raw files (Bronze)
    python run_pipeline.py all         # every layer that exists so far, in order
    python run_pipeline.py --help

Silver and Gold are added here as new stages when they are built
(one function per layer, same return contract: {"status": ..., "elapsed_time": ...}).
"""

import argparse                                   # read the command-line argument (which stage to run)
import logging                                    # log messages that show up in GitHub Actions
import sys                                        # exit code for the workflow
import time                                       # total run time

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("run_pipeline")


def run_bronze():
    """Bronze: download/copy raw source files and update the manifest."""
    from bronze_pipeline import run_bronze as bronze   # imported here so --help works without side effects
    return bronze()


# Stages in the order they must run. Add "silver" and "gold" here later.
STAGES = {
    "bronze": run_bronze,
}


def main():
    parser = argparse.ArgumentParser(description="DAT535 Group 24 pipeline")
    parser.add_argument("stage", choices=list(STAGES) + ["all"], help="stage to run")
    args = parser.parse_args()

    to_run = list(STAGES) if args.stage == "all" else [args.stage]   # "all" = every stage in order
    start = time.time()
    for name in to_run:
        logger.info("=" * 60)
        logger.info(f"RUNNING STAGE: {name.upper()}")
        logger.info("=" * 60)
        try:
            result = STAGES[name]()
        except Exception as e:                       # unexpected crash -> fail the run
            logger.exception(f"Stage {name} crashed: {e}")
            return 1
        logger.info(f"Stage {name}: {result}")
        if result.get("status") != "success":        # stop: later layers must not run on bad input
            logger.error(f"Stage {name} failed - stopping.")
            return 1
    logger.info(f"All done in {time.time() - start:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
