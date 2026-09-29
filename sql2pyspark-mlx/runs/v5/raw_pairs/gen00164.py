from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .filter(F.year(F.col("s.shipped_date")) >= 2023)
    .select(
        F.col("s.id"),
        F.col("s.order_id"),
        F.col("s.shipped_date"),
        F.col("o.channel"),
        F.col("st.sqft"),
    )
)
