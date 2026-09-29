from pyspark.sql import functions as F

result = (
    df_subscriptions
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupingSets(
        [["city", "country"], ["city"], []],
        F.col("c.city").alias("city"),
        F.col("c.country").alias("country"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("a.monthly_fee")).alias("total_monthly_fee"),
    )
    .orderBy(F.col("country").asc_nulls_first())
)
