from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.shipped_date"),
        F.col("s.delivered_date"),
        F.col("o.shipping_fee"),
        F.col("st.region"),
        F.col("st.opened_date"),
    )
    .orderBy(F.col("opened_date").desc(), "region", "id")
    .limit(50)
)
