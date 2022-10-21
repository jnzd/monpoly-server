from db_helper import DbHelper
import argparse
import log_events
from cli import cli

db = DbHelper()
monpoly: str



def main():
    return cli(db)


if __name__ == "__main__":
    main()
