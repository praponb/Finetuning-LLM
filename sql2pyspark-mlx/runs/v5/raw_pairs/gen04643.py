from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("merchant")
w2 = Window.orderBy("amount", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("account_id").orderBy(F.col("days_since_txn_time").desc_nulls_last())

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "txn_time").alias("days_since_txn_time"),
        F.coalesce("merchant", F.lit("N/A")).alias("resolved_merchant"),
    )
)

result = (
    valid
    .select(
        "*",
        F.count("amount").over(w_recent).alias("merchant_count_amount"),
        F.avg("days_since_txn_time").over(w2).alias("days_since_txn_time_avg_2_prior"),
        F.cume_dist().over(w3).alias("days_since_txn_time_cume_dist"),
    )
)
