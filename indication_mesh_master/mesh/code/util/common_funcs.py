# -*- coding: utf-8 -*-
"""
MeSH Master - top level common functions

Created on Fri 13 Sep 2024

@author: Mark Ediger
"""

# get global variables from config file
import sys
import pandas as pd

sys.path.append('../')
import settings.build_config as config

ctx = config.ctx

def snowflakeExecuteQuery(sf_query):
    try:
        cs = ctx.cursor()
        cs.execute(sf_query)
        results = cs.fetch_pandas_all()
        return(results)
    finally:
        cs.close()
    ctx.close()

def expand_tree_numbers(tree_number):
    if tree_number is None:
        return None
    parts = tree_number.split('.')
    parent_trees = ['.'.join(parts[:i+1]) for i in range(len(parts))]
    return list(dict.fromkeys(parent_trees))