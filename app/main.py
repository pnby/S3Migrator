import argparse

from app.api.backup import BackupManager


def main():
    parser = argparse.ArgumentParser(description='Backup management script')
    parser.add_argument('--source-dir', type=str, required=True, help='Source directory to backup')
    parser.add_argument('--dest-dir', type=str, required=True, help='Destination directory for backup tar files')
    parser.add_argument("--time", type=str, required=True, help="The time when data migration will begin, ex:'10:00'")

    args = parser.parse_args()

    backup_manager = BackupManager(
        source_dir=args.source_dir,
        dest_dir=args.dest_dir,
        timestamp=args.time
    )

    backup_manager.start_upload_task(args.time)


if __name__ == '__main__':
    main()
