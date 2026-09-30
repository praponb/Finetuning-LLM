from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "left")
    .filter(F.col("st.region").isin(["West", "Central", "North"]))
    .select(F.col("s.id"), F.col("s.order_id"), F.col("o.status"), F.col("st.region"))
    .orderBy(F.col("order_id").desc(), "id")
    .limit(10)
)
