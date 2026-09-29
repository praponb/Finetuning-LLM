from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.channel").isin(["partner", "mobile"]))
    .groupBy(
        F.col("s.city"),
        F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"),
    )
    .agg(
        F.sum(F.when(F.col("o.channel") == "partner", 1).otherwise(0)).alias("partner_count"),
    )
    .orderBy("city", "partner_count")
)
