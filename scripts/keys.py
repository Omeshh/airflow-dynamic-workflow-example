"""
Script used to:
generate encryption key for local credentials.
encode secret.
decode secret.

usage:
python keys.py -h
"""
import os
import json
import time
import argparse
from cryptography.fernet import Fernet

file_name = 'enc-key-local'
script_path = os.path.dirname(__file__)
keys_dir = os.path.join(script_path, '../keys/')
key_file = os.path.join(keys_dir, file_name + '.json')


def generate_key():
    key = Fernet.generate_key()

    if os.path.exists(key_file):
        back_file = os.path.join(keys_dir, file_name + '_%s.json' % time.strftime("%Y%m%d_%H%M%S"))
        os.rename(key_file, back_file)

    with open(key_file, 'w') as outfile:
        json.dump({'key': key.decode('ascii', 'ignore')}, outfile)
    print('Key file generated at: ' + key_file)


def enc(sec):
    with open(key_file) as f:
        key = json.load(f).get("key", "").encode('ascii', 'ignore')

    f = Fernet(key)
    token = f.encrypt(sec.encode('ascii', 'ignore'))
    print(token.decode('ascii', 'ignore'))


def dec(sec):
    with open(key_file) as f:
        decr = Fernet(json.load(f).get("key", "").encode('ascii', 'ignore'))

    passs = decr.decrypt(sec.encode('ascii', 'ignore')).decode('ascii', 'ignore')
    print(passs)


parser = argparse.ArgumentParser()
parser.add_argument("-e", "--encode", action='store_true', default=False, help="Encode a secret")
parser.add_argument("-d", "--decode", action='store_true', default=False, help="Decode a secret")
parser.add_argument("-g", "--generate_key", action='store_true', default=False, help="Generate a new Fernet key")
parser.add_argument('secret', nargs='?', default='')
args = parser.parse_args()

if args.generate_key:
    generate_key()
elif args.encode:
    enc(args.secret)
elif args.decode:
    dec(args.secret)
