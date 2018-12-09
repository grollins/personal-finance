import pandas as pd
from fuzzywuzzy import process
from merchants import MERCHANTS_BY_SPENDING_CATEGORY, CHASE_CHECKS
from util import fuzzy_match_factory, aggregate_over_time_freq, categorize_checks_factory
from pdb import set_trace

'''
Load data
'''
geoff_cap_one = pd.read_csv('data/geoff_capital_one_20180101_20181031.csv',
                            parse_dates=['Transaction Date', 'Posted Date'])
amex = pd.read_csv('data/amex_20180101_20181130.csv', header=None,
                   parse_dates=[0])
# chase csv was malformed: I had to change Check or Slip in the header to
# Check,Slip in order for the file to parse correctly
chase_checking = pd.read_csv('data/chase_checking_20180101_20181130.csv',
                             parse_dates=['Posting Date'])

'''
Conform columns
'''
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

chase_checking.columns = ['transaction class', 'date', 'merchant', 'amount',
                          'type', 'balance', 'check number', 'slip number']
chase_checking['account'] = 'Chase Checking'
chase_checking['card holder'] = 'joint'
chase_checking['amount'] *= -1

'''
Ignore credits for now
'''
geoff_cap_one2 = geoff_cap_one[geoff_cap_one['amount'] >= 0.0]
amex2 = amex[amex['amount'] >= 0.0]
chase_checking2 = chase_checking[chase_checking['amount'] >= 0.0]

'''
Remove unused columns
'''
columns_to_keep = ['date', 'account', 'merchant', 'card holder', 'amount']
geoff_cap_one3 = geoff_cap_one2[columns_to_keep]
amex3 = amex2[columns_to_keep]
chase_checking3 = chase_checking2[columns_to_keep]

'''
Union data
'''
df = pd.concat([geoff_cap_one3, amex3, chase_checking3]).reset_index(drop=True)
df['merchant'] = df.merchant.apply(lambda x: x.lower().strip())

'''
Map merchants to spending categories
'''
fuzzy_match_fcn = fuzzy_match_factory(MERCHANTS_BY_SPENDING_CATEGORY)
df['category'] = df['merchant'].map(fuzzy_match_fcn)

categorize_checks_fcn = categorize_checks_factory(CHASE_CHECKS)
df['category'] = df.apply(categorize_checks_fcn, axis=1)

print(df['category'].value_counts())
print(df.groupby('category')['amount'].sum())

df2 = df[df['category'] == 'other']
print(df2[['merchant', 'category']])

df2 = df[df['category'] == 'check']
print(df2[['merchant', 'date', 'amount']])

'''
Analysis

# spend per month total
print(aggregate_over_time_freq(df, group_col=None, dt_col='date',
                               freq='M', value_col='amount'))

# spend per month by category
print(aggregate_over_time_freq(df, group_col='category', dt_col='date',
                               freq='M', value_col='amount'))
'''
