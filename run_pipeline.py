#!/usr/bin/env python3
"""
DAT535 Group 24 - pipeline orchestrator
========================================

One entry point for every layer, used both on the server and by GitHub Actions.

Usage:
    python run_pipeline.py bronze      # sources -> raw files in the landing folder (/tmp/norgespris-landing/<env>)
    python run_pipeline.py postgres    # landing files -> PostgreSQL schema bronze (the Bronze layer)
    python run_pipeline.py silver      # bronze schema -> silver schema (clean, typed, daily)
    python run_pipeline.py gold        # silver schema -> gold schema (star schema + dashboard tables, checked, published)
    python run_pipeline.py all         # every stage, in order: bronze -> postgres -> silver -> gold
    python run_pipeline.py --help

One function per layer, same return contract: {"status": ..., "elapsed_time": ...}.
A stage that does not return "success" stops the run, so a later layer never
runs on bad input (Gold is never rebuilt from a failed Silver run).
"""

import argparse                                   # read the command-line argument (which stage to run)
import logging                                    # log messages that show up in GitHub Actions
import sys                                        # exit code for the workflow
import time                                       # total run time

logging.basicConfig(level=logging.INFO,                         # log format for all stages ...
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")  # ... time - logger - level - message
logger = logging.getLogger("run_pipeline")                      # own logger name for the orchestrator


def run_bronze():                                               # stage 1 (landing)
    """Download/copy raw source files into the landing folder and update the manifest."""
    from bronze_pipeline import run_bronze as bronze   # imported here so --help works without side effects
    return bronze()                                             # run it and return its status dict


def run_postgres():                                             # stage 2 (Bronze in PostgreSQL)
    """Bronze: load the newest landing files into PostgreSQL tables (schema bronze)."""
    from bronze_to_postgres import run_bronze_to_postgres   # imports Spark only when this stage runs
    return run_bronze_to_postgres()                             # run it and return its status dict


def run_silver():                                               # stage 3 (Silver)
    """Silver: clean, standardise and enrich the bronze tables into the silver schema."""
    from silver_pipeline import run_silver as silver   # imports Spark only when this stage runs
    return silver()                                             # run it and return its status dict


def run_gold():                                                 # stage 4 (Gold)
    """Gold: star schema + dashboard tables from the silver schema; published only if all checks pass."""
    from gold_pipeline import run_gold as gold   # imports Spark only when this stage runs
    return gold()                                               # run it and return its status dict


# Stages in the order they must run.
STAGES = {                                                      # stage name -> function, in run order
    "bronze": run_bronze,       # 1. sources -> raw files in /tmp/norgespris-landing/<env>
    "postgres": run_postgres,   # 2. landing files -> PostgreSQL norgespris_<env>, schema bronze
    "silver": run_silver,       # 3. schema bronze -> schema silver (same database)
    "gold": run_gold,           # 4. schema silver -> schema gold (via gold_stage, old version kept in gold_previous)
}                                                               # end of the stage list


def main():                                                     # command-line entry point
    parser = argparse.ArgumentParser(description="DAT535 Group 24 pipeline")  # parser for the command line
    parser.add_argument("stage", choices=list(STAGES) + ["all"], help="stage to run")  # one argument: which stage (or all)
    args = parser.parse_args()                                  # read the command line

    to_run = list(STAGES) if args.stage == "all" else [args.stage]   # "all" = every stage in order
    start = time.time()                                         # start time of the whole run
    for name in to_run:                                         # run the chosen stages one by one
        logger.info("=" * 60)                                   # separator line in the log
        logger.info(f"RUNNING STAGE: {name.upper()}")           # which stage starts
        logger.info("=" * 60)                                   # separator line in the log
        try:                                                    # catch crashes of a stage
            result = STAGES[name]()                             # run the stage function
        except Exception as e:                       # unexpected crash -> fail the run
            logger.exception(f"Stage {name} crashed: {e}")      # log the full error with traceback
            return 1                                            # exit code 1 = failed run
        logger.info(f"Stage {name}: {result}")                  # log the stage's summary
        if result.get("status") != "success":        # stop: later layers must not run on bad input
            logger.error(f"Stage {name} failed - stopping.")    # say why the run stops
            return 1                                            # exit code 1 = failed run
    logger.info(f"All done in {time.time() - start:.1f}s")      # total run time
    return 0                                                    # exit code 0 = success


if __name__ == "__main__":                                      # only when run as a script
    sys.exit(main())                                            # exit with the code from main()
