from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 76000, "HIGH").otherwise("LOW").alias("balance_tier"),
    )
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.count_if(F.col("status") == "FROZEN").alias("frozen_events"),
    )
    .orderBy("num_status")
)
