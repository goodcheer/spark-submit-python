import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.functions import PandasUDFType
"""
for spark 2.4.x , use PandasUDFType
"""


@pandas_udf("DOUBLE", PandasUDFType.GROUPED_AGG)
def sum_of_squares(series: pd.Series):
    """calculate sum of squares """
    return (series - series.mean()).pow(2).sum()


@pandas_udf("BIGINT", PandasUDFType.GROUPED_AGG)
def sum_(series: pd.Series):
    return series.sum()


# for spark later than 3.0, use python built-in type hint
# @pandas_udf("DOUBLE")
# def sum_of_squares(series: pd.Series) -> float:
#     """calculate sum of squares """
#     return (series - series.mean()).pow(2).sum()
#
#
# @pandas_udf("BIGINT")
# def sum_(series: pd.Series) -> int:
#     return series.sum()
