from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "inner")
    .filter(F.lower(F.trim(F.col("st.store_name"))) == "downtown express")
    .groupBy(F.col("st.region"))
    .agg(F.count(F.when(F.col("s.status") == "DELIVERED", 1)).alias("delivered_count"))
    .filter(F.col("delivered_count") > 4)
)
