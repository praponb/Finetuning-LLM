from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "inner")
    .filter(F.col("st.region").isin(["South", "West", "Central"]) | (F.col("st.city") == "Tokyo"))
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("st.city"),
        F.col("st.sqft"),
        F.date_add(F.col("o.order_date"), 75).alias("order_date_plus_75d"),
    )
)
