from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "inner")
    .filter(F.col("st.opened_date") >= "2025-07-01")
    .groupBy(F.col("o.channel"))
    .agg(F.countDistinct(F.col("s.cost")).alias("unique_orders"))
    .orderBy(F.col("unique_orders").desc())
)
