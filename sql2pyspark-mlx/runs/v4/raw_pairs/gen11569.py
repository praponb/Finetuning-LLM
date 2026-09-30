from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .filter(F.col("s.shipped_date").between("2024-11-12", "2025-01-24"))
    .select(
        F.col("s.status"),
        F.col("s.order_id"),
        F.col("o.discount"),
        F.col("o.status").alias("order_status"),
        F.col("st.store_name"),
        F.col("st.opened_date"),
    )
    .orderBy("order_status", F.col("store_name").desc())
)
