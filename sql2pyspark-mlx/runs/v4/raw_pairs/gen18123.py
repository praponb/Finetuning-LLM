from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.store_name").like("%Expre%"))
    .groupBy(
        F.when(F.col("c.city").isin(["Bangkok", "Chiang Mai"]), "core").otherwise("other").alias("city_group"),
    )
    .agg(F.countDistinct(F.col("o.shipping_fee")).alias("unique_statuss"))
    .orderBy("city_group")
    .limit(10)
)
