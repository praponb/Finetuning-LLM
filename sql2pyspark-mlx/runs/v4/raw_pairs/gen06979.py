from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "inner")
    .select(
        F.col("s.status"),
        F.col("s.cost"),
        F.col("o.shipping_fee"),
        F.col("st.store_name"),
        F.col("st.sqft"),
    )
)
