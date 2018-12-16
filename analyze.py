import numpy as np
import pandas as pd
import itertools as it
from fuzzywuzzy import process
from merchants import MERCHANTS_BY_SPENDING_CATEGORY, CHASE_CHECKS
from util import fuzzy_match_factory, aggregate_over_time_freq, categorize_checks_factory
from pdb import set_trace

'''
Load data
'''
geoff_cap_one = pd.read_csv('data/geoff_capital_one_20180101_20181130.csv',
                            parse_dates=['Transaction Date', 'Posted Date'])
amex = pd.read_csv('data/amex_20180101_20181130.csv', header=None,
                   parse_dates=[0])
# chase csv was malformed: I had to change Check or Slip in the header to
# Check,Slip in order for the file to parse correctly
chase_checking = pd.read_csv('data/joint_chase_checking_20180101_20181130.csv',
                             parse_dates=['Posting Date'])
# this credit card is mostly amazon, so maybe use amazon data directly?
# dan_chase_cc = pd.read_csv('data/dan_chase_cc_20180901_20181130.csv',
#                            parse_dates=['Trans Date', 'Post Date'])
amazon = pd.read_csv('data/amazon_20180101_20181130.csv',
                     parse_dates=['Order Date'],
                     usecols=['Order Date', 'Item Total'])
schwab = pd.read_csv('data/schwab_checking_20180101_20181130.csv',
                     parse_dates=['Date'])

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

# dan_chase_cc.columns = ['transaction class', 'transaction date', 'date',
#                         'merchant', 'amount']
# dan_chase_cc['account'] = 'Chase Credit Card'
# dan_chase_cc['card holder'] = 'dan'
# dan_chase_cc['amount'] *= -1

amazon.columns = ['date', 'total_usd']
amazon['merchant'] = 'amazon.com'
amazon['account'] = 'Amazon'
amazon['card holder'] = 'joint'
amazon['amount'] = amazon['total_usd'].apply(lambda x: float(x[1:]))

schwab.columns = ['date', 'transaction class', 'check number', 'merchant',
                  'withdrawal', 'deposit', 'balance']
schwab['account'] = 'Schwab'
schwab['card holder'] = 'geoff'
schwab['deposit'] = schwab['deposit'].replace('[\$,]', '', regex=True).astype(float) * -1
schwab['withdrawal'] = schwab['withdrawal'].replace('[\$,]', '', regex=True).astype(float)
schwab['amount'] = schwab.deposit.combine_first(schwab.withdrawal)

'''
Ignore credits for now
'''
geoff_cap_one2 = geoff_cap_one[geoff_cap_one['amount'] >= 0.0]
amex2 = amex[amex['amount'] >= 0.0]
chase_checking2 = chase_checking[chase_checking['amount'] >= 0.0]
# dan_chase_cc2 = dan_chase_cc[dan_chase_cc['amount'] >= 0.0]
amazon2 = amazon[amazon['amount'] >= 0.0]
schwab2 = schwab[schwab['amount'] >= 0.0]

'''
Remove unused columns
'''
columns_to_keep = ['date', 'account', 'merchant', 'card holder', 'amount']
geoff_cap_one3 = geoff_cap_one2[columns_to_keep]
amex3 = amex2[columns_to_keep]
chase_checking3 = chase_checking2[columns_to_keep]
# dan_chase_cc3 = dan_chase_cc2[columns_to_keep]
amazon3 = amazon2[columns_to_keep]
schwab3 = schwab2[columns_to_keep]

'''
Union data
'''
df = (pd.concat([geoff_cap_one3, amex3, chase_checking3, amazon3, schwab3])
        .reset_index(drop=True))
df['merchant'] = df.merchant.apply(lambda x: x.lower().strip())

'''
Map merchants to spending categories
'''
fuzzy_match_fcn = fuzzy_match_factory(MERCHANTS_BY_SPENDING_CATEGORY)
df['category'] = df['merchant'].map(fuzzy_match_fcn)

categorize_checks_fcn = categorize_checks_factory(CHASE_CHECKS)
df['category'] = df.apply(categorize_checks_fcn, axis=1)

df = df[df['category'] != 'remove']

print(df['category'].value_counts())
print(df.groupby('category')['amount'].sum())

df2 = df[df['category'] == 'other']
print(df2[['merchant', 'category']])

df2 = df[df['category'] == 'check']
print(df2[['merchant', 'date', 'amount']])

'''
Analysis
'''
# spend per month total
spend_per_month = aggregate_over_time_freq(df, group_col=None, dt_col='date',
                                           freq='M', value_col='amount')
spend_per_month = spend_per_month.reset_index()
print(spend_per_month)

# spend per month by category
spend_per_month_and_category = \
    aggregate_over_time_freq(df, group_col='category', dt_col='date',
                               freq='M', value_col='amount')
spend_per_month_and_category = \
    (spend_per_month_and_category.reset_index()
                                 .sort_values(['category', 'date']))

date_category_iter = \
    it.product(spend_per_month.date.astype('str').values, df.category.unique())
date_category_possible_pairs = [c for c in date_category_iter]
date_category_possible_pairs_df = pd.DataFrame(date_category_possible_pairs,
                                               columns=('date', 'category'))
date_category_possible_pairs_df['amount'] = 0.0

# df.to_csv('output.csv', index=False)
# spend_per_month_and_category.to_csv('output2.csv', index=False, float_format='%.2f')
