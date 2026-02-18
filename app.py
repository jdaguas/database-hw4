import argparse
from sync import init_db, full_load, incremental, validate

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["init", "full-load", "incremental", "validate"])
    args = parser.parse_args()


    if args.command == "init":
        init_db()
        print("Init completed. You can now run 'full-load' to load all data, "
        "or 'incremental' to sync changes.")
    elif args.command == "full-load":
        full_load()
    elif args.command == "incremental":
        incremental()
    elif args.command == "validate":
        validate()
    else:
        print("Not implemented yet:", args.command)


if __name__ == "__main__":
    main()
