from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "outer")
    .filter(F.col("o.store_id") == 31)
    .select(
        F.col("s.cost"),
        F.col("s.status"),
        F.col("o.status").alias("order_status"),
        F.col("st.city"),
        F.col("st.sqft"),
    )
)
