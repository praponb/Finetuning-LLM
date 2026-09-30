from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "left")
    .filter((F.year(F.col("o.order_date")) == 2025) & (F.col("o.amount") < 1500))
    .select(
        F.col("s.delivered_date"),
        F.col("o.status"),
        F.col("o.store_id"),
        F.col("st.store_name"),
        F.col("st.sqft"),
    )
    .orderBy("delivered_date")
)
