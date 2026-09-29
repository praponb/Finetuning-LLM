from pyspark.sql import functions as F

base = (
    df_accounts
    .filter((F.col("opened_date") > "2025-09-01") | F.col("status").isin(["CLOSED", "OPEN"]))
)

summary = (
    base
    .groupBy("customer_id")
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.min("opened_date").alias("min_opened_date"),
    )
)

result = summary.filter(F.col("avg_balance") > 39)
