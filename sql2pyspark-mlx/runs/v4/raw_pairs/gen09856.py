from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "left")
    .filter(F.col("o.shipping_fee") <= 30)
    .groupBy(
        F.when(F.col("o.shipping_fee") >= 39, "high").when(F.col("o.shipping_fee") >= 8, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(F.avg(F.col("s.cost")).alias("avg_cost"))
    .orderBy("avg_cost", "shipping_fee_bucket")
    .limit(5)
)
