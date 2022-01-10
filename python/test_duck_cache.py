import duck_cache as dc
import pandas as pd


def test_equal_df():
    df1 = pd.DataFrame([[1, 2], [4, 4]], columns=[list("ab")])
    df2 = pd.DataFrame([[1, 2], [4, 4]], columns=[list("ab")])
    assert df1.equals(df2)
