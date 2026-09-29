from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupingSets(
        [["city", "status"], ["city"]],
        F.col("c.city").alias("city"),
        F.col("a.status").alias("status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("a.balance")), 2).alias("total_balance"),
        F.grouping_id().alias("level_id"),
    )
)
