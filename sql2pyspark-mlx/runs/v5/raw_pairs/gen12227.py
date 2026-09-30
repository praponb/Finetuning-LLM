from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "inner")
    .filter(~(F.col("s.order_id").isNotNull() & (F.col("st.region") == "Central")))
    .select(
        F.col("s.delivered_date"),
        F.col("o.order_date"),
        F.col("st.opened_date"),
        F.col("st.sqft"),
    )
    .orderBy("opened_date")
)
