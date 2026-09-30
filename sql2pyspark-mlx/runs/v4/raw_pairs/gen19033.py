from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "outer")
    .filter(F.col("st.store_name").like("%Riversi%") & F.col("o.channel").isin(["mobile", "partner"]))
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("s.shipped_date"),
        F.col("o.amount"),
        F.col("o.order_date"),
        F.col("st.region"),
    )
)
