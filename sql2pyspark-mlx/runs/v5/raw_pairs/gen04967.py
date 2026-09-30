from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "left")
    .filter(F.col("st.store_name").like("%Downt%"))
    .groupBy(F.col("o.customer_id"))
    .agg(F.count(F.when(F.col("s.carrier") == "Kerry", 1)).alias("kerry_count"))
    .orderBy(F.col("kerry_count").desc(), F.col("customer_id").desc())
)
