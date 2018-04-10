#!/usr/bin/env python
import argparse
import os

#
#  Script to be placed in containers to help with making mount points
#  and pulling / pushing to buckets (eg S3) to accomodate utilities
#  that are unaware of S3 / buckets in containers
#  (to run on things like AWS Batch)
#

def build_parser():
    parser = argparse.ArgumentParser(description="""
        Wrapper to pull from buckets, run a command, and push back to buckets.""")
    parser.add_argument(
        '--command',
        '-c',
        type=str,
        help="""
        Command to be run AFTER downloads BEFORE uploads. 
        Will be passed unaltered as a shell command."""
    )
    parser.add_argument(
        '--download-files',
        '-DF',
        nargs='+',
        help="""Format is
        bucket_file_uri:container_path:mode
        Where mode can be 'ro' or 'rw'.
        If 'rw' the file will be pushed back to the bucket after the command
        IF 'ro, the file will only be pulled from the bucket
        e.g: s3://bucket/key/path.txt:/mnt/inputs/path.txt:ro""",
    )
    parser.add_argument(
        '--upload-files',
        '-UF',
        nargs='+',
        help="""Format is
        container_path:bucket_file_uri
        Mode is presumed to be w. (If you want rw / a / use input in mode 'rw')
        e.g: /mnt/outputs/path.txt:s3://bucket/key/path.txt""",
    )

    return parser


def main():
    """Entrypoint for main script."""
    parser = build_parser()
    args = parser.parse_args()

if __name__ == "__main__":
    main()