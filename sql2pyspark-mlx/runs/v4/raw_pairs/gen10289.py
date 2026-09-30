from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "left")
    .filter(F.col("s.cost") > 87)
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("s.cost"),
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("st.store_name"),
    )
    .orderBy("delivered_date")
)
