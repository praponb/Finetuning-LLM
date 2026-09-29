from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant").orderBy("amount", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("txn_type").orderBy(F.col("amount_filled").desc_nulls_last())

cleaned = (
    df_transactions
    .filter((F.col("txn_time") > F.to_timestamp(F.lit("2023-08-05 08:00:00"))) & (~F.col("merchant").eqNullSafe(F.lit("Amazon"))))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.upper("txn_type").alias("txn_type_upper"),
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("resolved_merchant"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "amount",
        "merchant",
        F.avg("amount").over(w).alias("amount_avg_5_prior"),
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w2).alias("event_seq_2"),
        F.percent_rank().over(w3).alias("amount_filled_percent_rank"),
    )
)
