from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "left")
    .filter(F.col("s.status") == "LOST")
    .select(
        F.col("s.id"),
        F.col("s.shipped_date"),
        F.col("s.carrier"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
        F.col("st.city"),
        F.col("st.store_name"),
    )
)
