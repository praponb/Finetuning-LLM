from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("customer_id") == 25) | F.month("opened_date").isin([3, 6, 12]))
    .groupBy(
        F.when(F.col("status").isin(["FROZEN", "OPEN"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_closed"),
    )
    .filter(F.col("pct_closed") < 71)
    .orderBy("pct_closed")
)
