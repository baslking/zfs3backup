import argparse
import sys
import boto3

from zfs3backup.config import get_config


def download(bucket, name):

	try:
		bucket.download_fileobj(name, sys.stdout.buffer)
	except Exception as ex:
		print("Boto3 download_fileobj call failed",file=sys.stderr)
		print(ex)
    

def main():
    cfg = get_config()
    parser = argparse.ArgumentParser(
        description='Read a key from s3 and write the content to stdout',
    )
    parser.add_argument('name', help='name of S3 key')
    args = parser.parse_args()

    if cfg['ENDPOINT']== 'aws':   # boto3.resource makes an intelligent decision with the default url
        s3 = boto3.Session(profile_name=cfg['PROFILE']).resource('s3')
    else:
        s3 = boto3.Session(profile_name=cfg['PROFILE']).resource('s3',endpoint_url=cfg['ENDPOINT'])

    bucket = s3.Bucket(cfg['BUCKET'])

    download(bucket, args.name)

if __name__ == '__main__':
    main()
