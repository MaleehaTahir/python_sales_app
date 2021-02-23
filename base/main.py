import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F


def create_spark_views(spark: SparkSession, customer_location: str, product_location: str,
                       transaction_location: str):
    spark.read.csv(customer_location, header=True).createOrReplaceTempView("customers")
    spark.read.csv(product_location, header=True).createOrReplaceTempView("products")
    df = spark.read.option("inferSchema", "true").json(transaction_location)
    explode_col = "basket"
    input_cols = ["customer_id", "date_of_purchase"]
    cols_to_extract = ["product_id", "price"]
    df = df.select(*input_cols, explode_outer(explode_col).alias(explode_col))
    df = df.select(*input_cols, F.col(explode_col + '.' + cols_to_extract[0]).alias(cols_to_extract[0]),
                   F.col(explode_col + '.' + cols_to_extract[1]).alias(cols_to_extract[1]))
    df.createOrReplaceTempView("transactions")


def create_derived_views(spark: SparkSession, customer_location: str, product_location: str,
                       transaction_location: str):
    create_spark_views(spark, customer_location, product_location, transaction_location)


def create_summarised_view(spark: SparkSession):
    summary = spark.sql("""SELECT a.customer_id, c.loyalty_score,
                                a.product_id, p.product_category, count(*) purchase_count
                           from transactions a
                           LEFT OUTER JOIN customers c
                           ON a.customer_id = c.customer_id
                           LEFT OUTER JOIN products p
                           ON a.product_id = p.product_id
                           GROUP BY a.customer_id, c.loyalty_score,
                                a.product_id, p.product_category""")
    convert_to_pandas = summary.toPandas()
    # print(convert_to_pandas.to_string())
    return convert_to_pandas


def get_row_count(spark: SparkSession):
    get_row_count = spark.sql("""SELECT COUNT(*) row_count from transactions""").collect()[0]
    print(get_row_count)
    return get_row_count


if __name__ == "__main__":
    spark_session = (
        SparkSession.builder
        .master("local[15]")
        .appName("test_tractable")
        .config("spark.executorEnv.PYTHONHASHSEED", "0")
        .getOrCreate()
    )
    parser = argparse.ArgumentParser(description="this is a test")
    parser.add_argument("--customer_location", required=False, default="./customers.csv")
    parser.add_argument("--product_location", required=False, default="./products.csv")
    parser.add_argument("--transaction_location", required=False, default="./transactions")
    args = vars(parser.parse_args())

    create_derived_views(spark_session, args["customer_location"], args["product_location"],
                         args["transaction_location"])
    # create_summarised_view(spark_session)
    get_row_count(spark_session)


# customer_id
# loyalty_score
# product_id
# product_category
# purchase_coun
