from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "merchant").orderBy(F.col("amount_filled").desc_nulls_first(), "id")

base = (
    df_transactions
    .filter(F.col("txn_time") >= F.to_timestamp(F.lit("2023-04-19 21:00:00")))
    .select(
        "*",
        F.substring("txn_type", 1, 4).alias("txn_type_prefix"),
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
)

result = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "amount_filled",
    )
)
