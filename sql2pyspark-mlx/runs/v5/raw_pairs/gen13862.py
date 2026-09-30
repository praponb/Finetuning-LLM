from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("amount", "id").rowsBetween(-3, Window.currentRow)

cleaned = (
    df_transactions
    .filter(F.col("txn_type") == "CREDIT")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.date_trunc("month", "txn_time").alias("txn_time_period"),
        F.coalesce("merchant", F.lit("NONE")).alias("resolved_merchant"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "account_id",
        "amount",
        "is_flagged",
        "txn_time_period",
        "resolved_merchant",
        F.avg("amount").over(w).alias("running_avg_amount"),
        F.min("amount").over(w).alias("running_min_amount"),
        F.sum("amount").over(w).alias("running_sum_amount"),
        F.avg("amount").over(w2).alias("amount_avg_3"),
    )
)
