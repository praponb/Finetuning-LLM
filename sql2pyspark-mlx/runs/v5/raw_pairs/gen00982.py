from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time_year", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("txn_type", "account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("txn_time_year").desc(), "id")

filtered = (
    df_transactions
    .filter(F.col("txn_time") >= F.to_timestamp(F.lit("2023-05-01 03:00:00")))
    .select(
        "*",
        F.lower("merchant").alias("merchant_lower"),
        F.year("txn_time").alias("txn_time_year"),
        F.coalesce("merchant", F.lit("OTHER")).alias("merchant_clean"),
    )
)

result = (
    filtered
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "merchant_lower",
        F.avg("txn_time_year").over(w).alias("txn_time_year_avg_3_prior"),
        F.first("amount", ignorenulls=True).over(w2).alias("first_known_amount"),
        F.ntile(4).over(w3).alias("txn_time_year_bucket"),
    )
)
