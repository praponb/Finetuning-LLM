from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.customer_id") == 30)
    .select(
        F.col("o.customer_id"),
        F.col("s.region"),
        F.date_add(F.col("o.order_date"), 89).alias("order_date_plus_89d"),
    )
    .orderBy("customer_id", "region")
)
