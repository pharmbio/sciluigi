#!/usr/bin/env python
import argparse
import os
import sys
import re
import subprocess

#
#  Script to be placed in containers to help with making mount points
#  and pulling / pushing to buckets (eg S3) to accomodate utilities
#  that are unaware of S3 / buckets in containers
#  (to run on things like AWS Batch)
#


class BCW():
    INPUT_RE = re.compile(
        r'^(?P<bucket_provider>\w+)://(?P<bucket>[^/]+)/(?P<key>.+?(?P<bucket_fn>[^/]+))::(?P<container_path>[^\0]+)::(?P<mode>rw|ro)$'
    )
    OUTPUT_RE = re.compile(
        r'^(?P<container_path>[^\0]+)::(?P<bucket_provider>\w+)://(?P<bucket>[^/]+)/(?P<key>.+?(?P<bucket_fn>[^/]+))$'
    )
    VALID_BUCKET_PROVIDERS = (
        's3',
    )

    def __init__(self):
        parser = self.build_parser()
        args = parser.parse_args()

        if args.command:
            self.command = args.command
        elif os.environ.get('bcw_command'):
            self.command = os.environ.get('bcw_command').strip()
        else:
            raise Exception("""
            No command provided on command line or as an environmental variable (bcw_command)
            """)

        print(type(args.download_files))
        print(args.download_files)

        if not args.download_files:
            self.download_files = []
        else:
            raw_download_files = [f.strip() for f in args.download_files if f.strip() != ""]
            if len(raw_download_files) > 0:
                self.download_files = self.parse_download_files(raw_download_files)
            else:
                self.download_files = []

        if not args.upload_files:
            self.upload_files = []
        else:
            raw_upload_files = [f.strip() for f in args.upload_files if f.strip() != ""]
            if len(raw_upload_files) > 0:
                self.upload_files = self.parse_upload_files(raw_upload_files)
            else:
                self.upload_files = []

        # Download from the bucket
        self.download_files_from_bucket()

        # Run the command
        subprocess.run(
            args.command,
            shell=True
            )

        # Upload files
        self.upload_files_to_bucket()

    def build_parser(self):
        parser = argparse.ArgumentParser(description="""
            Wrapper to pull from buckets, run a command, and push back to buckets.
            example:
                bucket_command_wrapper.py -c 'echo hello' \
                -DF s3://bucket/key/path.txt::/mnt/inputs/path.txt::rw \
                s3://bucket/key/path2.txt::/mnt/inputs/path2.txt::ro \
                -UF /mnt/outputs/path.txt::s3://bucket/key/path.txt
            """)

        if len(sys.argv) < 2:
            parser.print_help()

        # Implicit else
        parser.add_argument(
            '--command',
            '-c',
            type=str,
            help="""
            Command to be run AFTER downloads BEFORE uploads.
            Please enclose in quotes.
            Will be passed unaltered as a shell command.
            Can also be provided as an environmental variable bcw_command"""
        )
        parser.add_argument(
            '--download-files',
            '-DF',
            action='append',
            help="""Format is
            bucket_file_uri::container_path::mode
            Where mode can be 'ro' or 'rw'.
            If 'rw' the file will be pushed back to the bucket after the command
            IF 'ro, the file will only be pulled from the bucket
            e.g: s3://bucket/key/path.txt::/mnt/inputs/path.txt::ro""",
        )
        parser.add_argument(
            '--upload-files',
            '-UF',
            action='append',
            help="""Format is
            container_path::bucket_file_uri
            Mode is presumed to be w. (If you want rw / a / use input in mode 'rw')
            e.g: /mnt/outputs/path.txt::s3://bucket/key/path.txt""",
        )

        return parser

    def parse_upload_files(self, raw_upload_files):
        upload_files = []
        for d in raw_upload_files:
            m = self.OUTPUT_RE.search(d.strip())
            if not m:
                raise Exception("Invalid upload file {}".format(d))
            # Implicit else
            bucket_provider = m.group('bucket_provider').lower()
            bucket = m.group('bucket')
            key = m.group('key')
            bucket_fn = m.group('bucket_fn')
            container_path = os.path.abspath(m.group('container_path'))

            # Be sure this is one of the providers we know how to handle
            if bucket_provider not in self.VALID_BUCKET_PROVIDERS:
                raise Exception("Invalid bucket provider {}. Valid choices are {}".format(
                    bucket_provider,
                    ", ".join(self.VALID_BUCKET_PROVIDERS)
                ))

            # Be sure we can create the path to the proposed container mount point
            try:
                os.makedirs(
                    os.path.dirname(
                        container_path
                        )
                    )
            except FileExistsError:
                # Fine if this path already exists
                pass

            upload_files.append({
                'bucket_provider':  bucket_provider,
                'bucket': bucket,
                'key':  key,
                'bucket_fn': bucket_fn,
                'container_path': container_path,
            })
        return(upload_files)

    def parse_download_files(self, raw_download_files):
        download_files = []
        for d in raw_download_files:
            m = self.INPUT_RE.search(d.strip())
            if not m:
                raise Exception("Invalid download file {}".format(d))
            # Implicit else
            bucket_provider = m.group('bucket_provider')
            bucket = m.group('bucket')
            key = m.group('key')
            bucket_fn = m.group('bucket_fn')
            container_path = os.path.abspath(m.group('container_path'))
            mode = m.group('mode').lower()

            # Be sure this is one of the providers we know how to handle
            if bucket_provider not in self.VALID_BUCKET_PROVIDERS:
                raise Exception("Invalid bucket provider {}. Valid choices are {}".format(
                    bucket_provider,
                    ", ".join(self.VALID_BUCKET_PROVIDERS)
                ))

            # Be sure we can create the path to the proposed container mount point
            try:
                os.makedirs(
                    os.path.dirname(
                        container_path
                        )
                    )
            except FileExistsError:
                # Fine if this path already exists
                pass

            download_files.append({
                'bucket_provider':  bucket_provider,
                'bucket': bucket,
                'key':  key,
                'bucket_fn': bucket_fn,
                'container_path': container_path,
                'mode': mode,
            })
        return(download_files)

    def download_file_s3(self, df):
        import boto3
        s3_client = boto3.client('s3')
        s3_client.download_file(
            Bucket=df['bucket'],
            Key=df['key'],
            Filename=df['container_path'],
            )

    def download_files_from_bucket(self):
        for df in self.download_files:
            if df['bucket_provider'] == 's3':
                self.download_file_s3(df)
            else:
                raise Exception("Invalid bucket provider {}".format(
                    df['bucket_provider'])
                    )

    def upload_file_s3(self, df):
        import boto3
        s3_client = boto3.client('s3')
        s3_client.upload_file(
            Bucket=df['bucket'],
            Key=df['key'],
            Filename=df['container_path'],
            )

    def upload_files_to_bucket(self):
        upload_files = self.upload_files+[f for f in self.download_files if f['mode']=='rw']
        for df in upload_files:
            if df['bucket_provider'] == 's3':
                self.upload_file_s3(df)
            else:
                raise Exception("Invalid bucket provider {}".format(
                    df['bucket_provider'])
                    )


def main():
    """Entrypoint for main script."""
    BCW()

if __name__ == "__main__":
    main()
