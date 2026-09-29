from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("c.is_active"))
    .select(
        F.col("o.status"),
        F.col("c.is_active"),
        F.col("c.segment"),
        F.col("s.region"),
        F.col("s.opened_date"),
        F.initcap(F.trim(F.col("c.country"))).alias("country_title"),
    )
    .orderBy("status")
)
