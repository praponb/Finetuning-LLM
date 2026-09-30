from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .filter((F.year(F.col("o.order_date")) >= 2023) | (F.col("o.discount") < 0.12))
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("s.shipped_date"),
        F.col("o.amount"),
        F.col("o.order_date"),
        F.col("st.store_name"),
    )
)
