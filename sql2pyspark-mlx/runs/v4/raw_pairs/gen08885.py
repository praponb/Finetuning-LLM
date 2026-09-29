from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter((F.col("o.status") != "PENDING") & F.col("s.store_name").like("Do%"))
    .select(F.col("o.status"), F.col("o.shipping_fee"), F.col("s.store_name"))
    .orderBy("status")
)
