from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .groupBy(
        F.when(F.col("o.shipping_fee") >= 30, "high").when(F.col("o.shipping_fee") >= 12, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(F.count(F.when(F.col("s.carrier") == "Flash", 1)).alias("flash_count"))
    .orderBy("shipping_fee_bucket")
)
