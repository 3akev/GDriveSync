#!/usr/bin/env python3

import argparse
import asyncio
import logging
from datetime import datetime

from client.cloner import GoogleDriveCloner
from client.cleaner import GoogleDriveCleaner
from client.browser import GoogleDriveBrowser
from client.diff import GoogleDriveDiff
from client.linker import GoogleDriveLinker
from client.quota import GoogleDriveQuota
from client.rotator import GoogleDriveRotator

from consts import logger


def diff_directories(args):
    diff = GoogleDriveDiff(args)
    asyncio.run(diff.run(args.first, args.second))


def print_quota(args):
    quota = GoogleDriveQuota(args)
    asyncio.run(quota.run())


def browse_files(args):
    browser = GoogleDriveBrowser(args)
    asyncio.run(browser.run(args.root, args.orphans))


def link_files(args):
    linker = GoogleDriveLinker(args)
    asyncio.run(linker.run(args.target, args.destination))


def cleanup_files(args):
    cleaner = GoogleDriveCleaner(args)
    asyncio.run(cleaner.run(*args.delete, dry_run=args.dry_run))


def clone_files(args):
    googledrivecloner = GoogleDriveCloner(args)
    asyncio.run(
        googledrivecloner.run(
            base_folder_id=args.source_folder_id,
            destination_parent_folder_id=args.destination_parent_folder_id,
            new_name=args.name,
            dry_run=args.dry_run,
        )
    )


def rotate_backups(args):
    google_backup_rotator = GoogleDriveRotator(args)
    asyncio.run(
        google_backup_rotator.run(
            base_folder_id=args.source_folder_id,
            destination_parent_folder_id=args.destination_parent_folder_id,
            new_name=args.name,
            dry_run=args.dry_run,
        )
    )


def parse_arguments():
    today = datetime.today().strftime("%Y-%m-%d")
    new_folder_name = f"drive_backup_{today}"

    parser = argparse.ArgumentParser(description="Copy a folder in Google Drive")
    parser.add_argument("-v", "--verbose", help="Increase output verbosity", action="store_true")
    parser.add_argument(
        "-vv",
        "--very-verbose",
        help="Increase output verbosity more. This is really spammy",
        action="store_true",
    )
    parser.add_argument("-q", "--quiet", help="Decrease output verbosity", action="store_true")

    creds_group = parser.add_mutually_exclusive_group(required=True)
    creds_group.add_argument("-s", "--secrets", help="Path to secrets directory or file")
    creds_group.add_argument("--oauth", help="Use OAuth2 credentials", action="store_true")

    parser.add_argument(
        "-a",
        "--account",
        help="Account to use if secrets directory",
        type=int,
        default=1,
    )

    subparsers = parser.add_subparsers()

    diff_parser = subparsers.add_parser("diff", help="Diff own directory with another directory")
    diff_parser.set_defaults(func=diff_directories)
    diff_parser.add_argument("first", help="First directory id")
    diff_parser.add_argument("second", help="Second directory id")

    quota_parser = subparsers.add_parser("quota", help="Get quota info")
    quota_parser.set_defaults(func=print_quota)

    browse_parser = subparsers.add_parser("browse", help="Browse files")
    browse_parser.set_defaults(func=browse_files)
    browse_parser.add_argument("root", help="Folder from which to start browsing", default="root", nargs="?")
    browse_parser.add_argument("--orphans", help="Browse orphan files", action="store_true")

    link_parser = subparsers.add_parser("link", help="Create shortcut files")
    link_parser.set_defaults(func=link_files)
    link_parser.add_argument("target", help="Target item to link")
    link_parser.add_argument("destination", help="Destination folder to link to", nargs="?")

    cleanup_parser = subparsers.add_parser("delete", help="Delete files")
    cleanup_parser.set_defaults(func=cleanup_files)
    cleanup_parser.add_argument("delete", help="File IDs to delete", nargs="+")
    cleanup_parser.add_argument("--dry-run", help="Print files to delete", action="store_true")

    backup_parser = subparsers.add_parser("clone", help="Copy files")
    backup_parser.set_defaults(func=clone_files)
    backup_parser.add_argument("--dry-run", help="Print files to copy", action="store_true")
    backup_parser.add_argument("--name", help="New folder name", default=new_folder_name)

    backup_parser.add_argument("source_folder_id", help="Folder ID to copy")
    backup_parser.add_argument("destination_parent_folder_id", help="Destination folder ID")

    rotate_parser = subparsers.add_parser("rotate", help="Rotate backups")
    rotate_parser.add_argument("--dry-run", help="Print files to copy", action="store_true")
    rotate_parser.add_argument("--name", help="New folder name", default=new_folder_name)
    rotate_parser.set_defaults(func=rotate_backups)
    rotate_parser.add_argument("source_folder_id", help="Folder ID to copy")
    rotate_parser.add_argument("destination_parent_folder_id", help="Destination folder ID")

    arguments = parser.parse_args()
    return arguments


def main():
    args = parse_arguments()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    elif args.very_verbose:
        logger.setLevel(logging.TRACE)  # type: ignore
    elif args.quiet:
        logger.setLevel(logging.WARNING)

    args.func(args)


if __name__ == "__main__":
    main()
