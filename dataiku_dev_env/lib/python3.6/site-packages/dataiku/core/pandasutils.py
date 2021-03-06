#!/usr/bin/env python
# encoding: utf-8
"""
utils.py

Created by Thomas Cabrol on 2013-06-28.
Copyright (c) 2013 Dataiku SAS. All rights reserved.
"""

import pandas as pd
import numpy as np
import random
from pandas.core.series import Series
from pandas.core.frame import DataFrame
from pandas import Categorical


def notnan_mask(df):
    arr = ~np.isnan(df.values)
    if len(arr.shape) == 2:
        return arr.all(axis=1)
    else:
        return arr


def audit(df):
    '''
    A simple function to make a base audit of any dataframe
    '''
    stats = []  # A container for all the stats
    for c in df.columns:
        u = df[c].unique()
        stat = {}  # A container for individual column stats
        stat['_a_variable'] = c  # Name of the variable
        stat['_b_data_type'] = df[c].dtype  # Data type
        stat['_c_cardinality'] = len(u)  # Number of unique values
        stat['_d_missings'] = df[c].isnull().sum()  # Number of missing values
        stat['_e_sample_values'] = u[0:2]  # Two sample values
        stats.append(stat)
    return pd.DataFrame(stats)


def cast_to_unicode(s):
    if isinstance(s, str):
        return s.lower().decode('utf-8')
    elif isinstance(s, unicode):
        return s
    return unicode(s)



def look_like_text(array):
    i = 0
    for value in array:
        if not (isinstance(value, str) or isinstance(value, unicode)):
            continue
        l = len([k for k in value if k in ['"', ',' '\'', ' ']])
        if  l * 10 > len(value):
            i = i + 1
    return i > len(array) / 2

def get_series_stats(series):
    dtype = str(series.dtype)
    u = series.unique()
    return {
        'variable': series.name,  # Name of the variable
        'data_type': series.dtype,
        'cardinality': len(u),
        'missings': series.isnull().sum(),
        'sample_values': u"; ".join([cast_to_unicode(s) for s in u[0:5]]),
        'sample_values_raw' : u[0:100],
        'type': 'NUMERIC' if ('int' in str(dtype) or 'float' in str(dtype)) else 'TEXT' if (len(u) > len(series) / 2 and look_like_text(u[0:100])) else 'CATEGORY',
        'role': 'INPUT'
    }
    

def get_stats(df):
    return [
        get_series_stats(df[col_name])
        for col_name in df.columns
    ]


def split_train_valid(df, prop=0.8, seed=None):
    '''
    A function that takes an input data frame df and splits it
    into 2 other data frames based on prop (defaults to 80% for
    the first one)
    '''
    k = int(df.shape[0] * prop)
    random.seed(seed)
    sampler = random.sample(df.index.tolist(), k)
    train = df.loc[sampler]
    valid = df[~df.index.isin(sampler)]
    return (train, valid)


def sample_without_replacement(df, prop=0.5):
    ''' Draw a random sample without replacement '''
    k = df.shape[0] * prop
    k = int(k)
    sampler = random.sample(df.index.tolist(), k)
    sample = df.loc[sampler]
    return sample


def sample_with_replacement(df, prop=1):
    ''' Draw a 100% sample (by default, adjustable) with replacement for bootstrapping purpose '''
    ''' It works, but it shouldn't '''
    k = df.shape[0] * prop
    k = int(k)
    sampler = np.random.choice(df.index.tolist(), k, replace=True)
    sample = df.loc[sampler]
    return sample


def _get_dummies(data, prefix=None, prefix_sep='_'):
    """
    Convert categorical variable into dummy/indicator variables

    Parameters
    ----------
    data : array-like or Series
    prefix : string, default None
        String to append DataFrame column names
    prefix_sep : string, default '_'
        If appending prefix, separator/delimiter to use

    Returns
    -------
    dummies : DataFrame
    """
    cat = Categorical.from_array(np.asarray(data))
    dummy_mat = np.eye(len(cat.levels)).take(cat.labels, axis=0)

    if prefix is not None:
        dummy_cols = ['%s%s%s' % (prefix, prefix_sep, printable(v))
                      for v in cat.levels]
    else:
        dummy_cols = cat.levels

    if isinstance(data, Series):
        index = data.index
    else:
        index = None

    return DataFrame(dummy_mat, index=index, columns=dummy_cols, dtype='uint8')


def set_column_sequence(dataframe, seq):
    '''Takes a dataframe and a subsequence of its columns, returns dataframe with seq as first columns'''
    cols = seq[:]  # copy so we don't mutate seq
    for x in dataframe.columns:
        if x not in cols:
            cols.append(x)
    return dataframe[cols]

