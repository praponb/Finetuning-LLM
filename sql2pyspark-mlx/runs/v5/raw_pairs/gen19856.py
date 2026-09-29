from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .filter(F.col("st.store_name").isNotNull())
    .groupBy(F.col("o.channel"), F.col("o.status"))
    .agg(F.count(F.when(F.col("s.carrier") == "Kerry", 1)).alias("kerry_count"))
)
