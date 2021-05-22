import argparse
from datetime import datetime

import boto

from zfs3backup.config import get_config


def cleanup_multipart(bucket, max_days=1, dry_run=False):
    max_age_seconds = max_days * 24 * 3600
    now = datetime.utcnow()
    print(f"{'A'} | {'key':30} | {'initiated':20}")
    for multi in bucket.list_multipart_uploads():
        delta = now-boto.utils.parse_ts(multi.initiated)
        if delta.total_seconds() >= max_age_seconds:
            print f"{'X'} | {multi.key_name:30} | {multi.initiated:20}"
            if not dry_run:
                multi.cancel_upload()
        else:
            print f"{' '} | {multi.key_name:30} | {multi.initiated:20}"


def main():
    cfg = get_config()
    parser = argparse.ArgumentParser(
        description='Cleanup hanging multipart s3 uploads',
    )
    parser.add_argument('--max-age',
                        dest='max_days',
                        default=1,
                        type=int,
                        help='maximum age in days')
    parser.add_argument('--dry',
                        dest='dry_run',
                        action='store_true',
                        help='Don\'t cancel any upload')
    args = parser.parse_args()
    bucket = boto.connect_s3(
        cfg['S3_KEY_ID'], cfg['S3_SECRET']).get_bucket(cfg['BUCKET'])
    cleanup_multipart(
        bucket,
        max_days=args.max_days,
        dry_run=args.dry_run,
    )


if __name__ == '__main__':
    main()
