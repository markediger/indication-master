#!/usr/bin/env python
"""
MeSH Master - Build Configuration file
contains local file path, global variables, and connection strings

Created on Fri 13 Sep 2024

@author: Mark Ediger
"""

"""
Script to connect to Snowflake using a locally stored private key
"""


import os
import logging
import snowflake.connector

# EXTRA LOGGING TO DEBUG CONNECTION
for logger_name in ['snowflake.connector', 'botocore', 'boto3']:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    ch = logging.FileHandler('python_connector.log')
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s %(filename)s:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s'))
    logger.addHandler(ch)

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

from dotenv import load_dotenv
load_dotenv()

sf_prv_key = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
sf_prv_pw = os.getenv("SNOWFLAKE_KEY_PSWD")

with open(sf_prv_key, "rb") as key:
    p_key= serialization.load_pem_private_key(
        key.read(),
        password=sf_prv_pw.encode(),
        # password=None,
        backend=default_backend()
    )

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption())


ctx = snowflake.connector.connect(
    private_key=pkb,
    user='mediger',
    account='vaa16628.us-east-1',
    warehouse='ACORN_DSE_PROD_VW',
    database='ACORN_DSE_PROD_MDR',
    schema='UMLS',
    role='ACORN_DSE_PROD_IE_RO'
    )


