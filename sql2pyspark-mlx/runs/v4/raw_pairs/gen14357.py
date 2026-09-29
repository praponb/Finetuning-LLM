from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id")
w2 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(-6, Window.currentRow)

with_windows = (
    df_accounts
    .select(
        "*",
        F.lead("balance", 1, 0).over(w).alias("next_balance"),
        F.avg("balance").over(w2).alias("balance_avg_6"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("balance_avg_6") >= 68449, "HIGH").otherwise("LOW").alias("balance_avg_6_tier"),
    )
    .agg(
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_rows"),
        F.countDistinct("status").alias("distinct_status"),
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance")).otherwise(0)).alias("frozen_balance"),
    )
    .orderBy(F.col("savings_rows").desc_nulls_last())
)
