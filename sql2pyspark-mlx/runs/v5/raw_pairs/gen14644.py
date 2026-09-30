from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.channel").isin(["partner", "mobile", "store"]))
    .groupBy(
        F.col("c.segment"),
        F.when(F.col("c.city").isin(["Tokyo", "Singapore"]), "tier_1").otherwise("other").alias("city_group"),
    )
    .agg(
        F.round(F.stddev(F.col("o.amount")), 2).alias("stddev_amount"),
        F.min(F.col("o.discount")).alias("min_discount"),
    )
    .filter(F.col("min_discount") >= 0.39)
    .orderBy("segment", F.col("city_group").desc())
    .limit(5)
)
