import pandas as pd
from fuzzywuzzy import process

def fuzzy_match_factory(choice_dict, score_cutoff=80):
    def fuzzy_match(s):
        """Fuzzy match string to possible values given by the keys of a
        dictionary. Find the key that is the best match for the string, and then
        return the value that corresponds to that key in the dictionary.
        """
        m = process.extractOne(s, choices=choice_dict.keys(),
                               score_cutoff=score_cutoff)
        if m:
            top_hit_key = m[0]
            return choice_dict[top_hit_key]
        else:
            return ''
    return fuzzy_match

def aggregate_over_time_freq(df, group_col='group', dt_col='date', freq='M',
                             value_col='count'):
    """
    Sum values by group over a given time frequency, e.g. monthly
    http://pbpython.com/pandas-grouper-agg.html
    http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases
    """
    g = pd.Grouper(key=dt_col, freq=freq)
    return df.groupby([group_col, g])[value_col].sum()
