from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        "status",
        F.when(F.col("balance") >= 88000, "HIGH").otherwise("LOW").alias("balance_tier"),
    )
    .agg(
        F.min("balance").alias("min_balance"),
        F.count_if(F.col("status") == "CLOSED").alias("closed_events"),
        F.count_if(F.col("status") == "CLOSED").alias("closed_events_2"),
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("account_type") == "CREDIT", 1)), F.lit(0)), 2).alias("balance_per_credit"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
    )
    .filter(F.col("balance_per_credit") < 0)
)
