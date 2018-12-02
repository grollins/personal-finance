import pandas as pd
from fuzzywuzzy import process
from merchants import MERCHANTS_BY_SPENDING_CATEGORY
from util import fuzzy_match_factory, aggregate_over_time_freq


df = pd.read_csv('data/geoff_capital_one_20180101_20181031.csv',
                 parse_dates=['Transaction Date', 'Posted Date'])
df2 = df[df['Credit'].isna()]

fuzzy_match_fcn = fuzzy_match_factory(MERCHANTS_BY_SPENDING_CATEGORY)
df2['Spending Category'] = df2['Description'].map(fuzzy_match_fcn)
print(df2['Spending Category'].value_counts())

print(df2.groupby('Spending Category')['Debit'].sum())

'''
aggregate_over_time_freq(df2, group_col='Spending Category', dt_col='Posted Date',
                         freq='M', value_col='Debit')
'''
