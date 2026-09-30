from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.lower(F.trim(F.col("c.email"))) == "jack@startup.io")
    .groupBy(F.quarter(F.col("s.opened_date")).alias("opened_quarter"))
    .agg(
        F.sum(F.when(F.col("o.status") == "SHIPPED", 1).otherwise(0)).alias("shipped_count"),
    )
)
