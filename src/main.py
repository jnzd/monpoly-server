from db_helper import db_helper
import argparse
import log_events
from cli import cli

db = db_helper()
monpoly: str



def main():
    return cli(db)


if __name__ == "__main__":
    main()
