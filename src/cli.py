from db_helper import db_helper
import argparse
import log_events

def cli(db: db_helper):
    parser = argparse.ArgumentParser()

    # --create-database <signature-file> --log-event events 
    parser.add_argument("-c", "--create-database",
                        dest = "signature",
                        help="Takes the path to a signature file and creates a corresponding database")

    parser.add_argument("-l", "--log-event",
                        nargs='*',
                        dest = "events",
                        help="Takes a list of events and adds them to the database with the current time")
    
    parser.add_argument("-t", "--timestamp",
                        dest="timestamp",
                        help="uses the given timestamp for the reported events. If not given, the current time is used")

    parser.add_argument("-d", "--delete-database",
                        dest = "signature_delete",
                        help="DANGER! Takes the path to a signature file and deletes all tables associated with it")

    args = parser.parse_args()
    # db = db_helper()

    if (args.signature): db.create_database(args.signature)
    elif (args.events):
        if (args.timestamp):
            log_events.log_events(args.events, args.timestamp)
        else:
            log_events.log_events(args.events)
    elif (args.signature_delete): db.delete_database(args.signature_delete)
    