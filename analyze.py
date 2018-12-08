import pandas as pd
from fuzzywuzzy import process
from merchants import MERCHANTS_BY_SPENDING_CATEGORY
from util import fuzzy_match_factory, aggregate_over_time_freq


# Load data
geoff_cap_one = pd.read_csv('data/geoff_capital_one_20180101_20181031.csv',
                            parse_dates=['Transaction Date', 'Posted Date'])
amex = pd.read_csv('data/amex_20180101_20181130.csv', header=None,
                   parse_dates=[0])

# Conform columns
geoff_cap_one.columns = ['transaction date', 'date', 'card number', 'merchant',
                          'category', 'debit', 'credit']
geoff_cap_one['account'] = 'Capital One'
geoff_cap_one['card holder'] = 'geoff'
geoff_cap_one['credit'] *= -1
# coalesce debit and credit columns into one amount column
geoff_cap_one['amount'] = geoff_cap_one.debit.combine_first(geoff_cap_one.credit)

amex.columns = ['date', 'unk1', 'merchant', 'card holder full name', 'card number',
                'unk5', 'unk6', 'amount', 'unk8', 'unk9', 'unk10', 'unk11',
                'unk12', 'unk13', 'unk14', 'unk15']
amex['account'] = 'American Express'
amex['card holder'] = amex['card holder full name'].apply(lambda x: 'dan' if x == 'Dan Lu' else 'geoff')

# Ignore credits for now
geoff_cap_one2 = geoff_cap_one[geoff_cap_one['amount'] >= 0.0]
amex2 = amex[amex['amount'] >= 0.0]

# Remove unused columns
columns_to_keep = ['date', 'account', 'merchant', 'card holder', 'amount']
geoff_cap_one3 = geoff_cap_one2[columns_to_keep]
amex3 = amex2[columns_to_keep]

# Union data
df = pd.concat([geoff_cap_one3, amex3])

# fuzzy_match_fcn = fuzzy_match_factory(MERCHANTS_BY_SPENDING_CATEGORY)
# df2['Spending Category'] = df2['Description'].map(fuzzy_match_fcn)
# print(df2['Spending Category'].value_counts())
#
# print(df2.groupby('Spending Category')['Debit'].sum())

'''
aggregate_over_time_freq(df2, group_col='Spending Category', dt_col='Posted Date',
                         freq='M', value_col='Debit')
'''
