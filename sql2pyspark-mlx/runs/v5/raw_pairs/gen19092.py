from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("account_id", "txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

with_windows = (
    df_transactions
    .filter(~F.col("txn_type").isin(["REFUND", "DEBIT", "FEE"]))
    .select(
        "id",
        "is_flagged",
        F.avg("amount").over(w_running).alias("amount_avg_6"),
        F.sum("amount").over(w2).alias("amount_sum_30d"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("amount_avg_6") >= 3323, "PREMIUM").otherwise("STANDARD").alias("amount_avg_6_tier"),
    )
    .agg(
        F.round(F.avg("amount_avg_6"), 2).alias("avg_amount_avg_6"),
        F.percentile_approx("amount_sum_30d", [0.25, 0.5, 0.75]).alias("amount_sum_30d_quartiles"),
        F.percentile_approx("amount_avg_6", 0.5).alias("amount_avg_6_p50"),
    )
    .orderBy(F.col("avg_amount_avg_6").desc_nulls_last())
)
