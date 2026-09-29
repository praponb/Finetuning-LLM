from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.customer_id").isNull())
    .groupBy(
        F.when(F.col("c.city").isin(["Bangkok", "Chiang Mai"]), "core").otherwise("other").alias("city_group"),
    )
    .agg(
        F.avg(F.col("a.balance")).alias("avg_balance"),
        F.round(F.stddev(F.col("a.balance")), 2).alias("stddev_balance"),
    )
    .orderBy(F.col("avg_balance").desc())
)
