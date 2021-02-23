from base.main import get_row_count
import pytest
import pandas as pd

@pytest.mark.usefixtures("spark")
def test_match_row_count(spark):
    input = 4497
    output = list(range(input + 1))
    df = pd.DataFrame(output)
    spark.createDataFrame(df).createOrReplaceTempView("transactions")

    expected = spark.sql("""SELECT COUNT(*) row_count from transactions""").collect()[0]
    actual = get_row_count(spark)

    assert actual == expected






