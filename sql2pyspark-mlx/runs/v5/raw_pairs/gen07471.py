from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "right")
    .select(
        F.col("s.id"),
        F.col("s.status"),
        F.col("s.order_id"),
        F.col("o.shipping_fee"),
        F.col("st.store_name"),
        F.col("st.sqft"),
        F.substring(F.col("st.city"), 1, 3).alias("city_prefix"),
    )
    .orderBy("status")
)
