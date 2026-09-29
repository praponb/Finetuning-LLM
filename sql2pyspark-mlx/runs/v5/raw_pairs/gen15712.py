from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_accounts
    .filter(F.col("customer_id") == 20)
    .select(
        "id",
        "balance",
        "status",
        F.sum("balance").over(w).alias("running_sum_balance"),
        F.count("balance").over(w).alias("running_count_balance"),
        F.min("balance").over(w).alias("running_min_balance"),
        F.avg("balance").over(w).alias("running_avg_balance"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("running_sum_balance") >= 345000, "LARGE").otherwise("SMALL").alias("running_sum_balance_tier"),
        "status",
    )
    .agg(
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_rows"),
        F.percentile_approx("running_count_balance", [0.25, 0.5, 0.75]).alias("running_count_balance_quartiles"),
        F.round(F.sum("running_avg_balance") / F.nullif(F.count(F.when(F.col("status") == "FROZEN", 1)), F.lit(0)), 2).alias("running_avg_balance_per_frozen"),
    )
    .orderBy(F.col("frozen_rows").desc())
)
