# GDriveSync

This repository contains an async wrapper over the Google Drive API,
with support for request batching, and a collection of "clients" that
perform specific operations on the API.

Runs on python 3.11.8 and 3.12.3 and the library versions in `requirements.txt`. Other
versions may work but are not tested.

## Usage

The main entry point is `gdrive.py`. It has a subcommand per client, so use
`--help` to see the available clients and their options.

## Notes

- The option `--secrets` can be a path to a service account secret file, or a
  directory containing such files. In the latter case, the client will use the
  option `--account` to select the account to use, or by default the first one.

## Clients


| Client      | Description                                                                                     |
|-------------|-------------------------------------------------------------------------------------------------|
| `diff`      | Output difference between two given folder ids, such as modified and new files on either side.  |
| `quota`     | Prints out storage quota limits for used account                                                |
| `browse`    | See files inside given folder id                                                                |
| `link`      | Create shortcut file pointing to source, and place it as a child of target                      |
| `delete`    | Delete given file ids                                                                           |
| `backup`    | Clone source folder to and place it as a child of destination folder                            |
| `rotate`    | Rotate backups. Calls `delete` and `backup`                                                     |

