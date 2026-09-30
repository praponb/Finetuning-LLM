from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "inner")
    .groupBy(
        F.when(F.col("o.channel").isin(["mobile", "partner"]), "tier_1").otherwise("other").alias("channel_group"),
    )
    .agg(
        F.count(F.col("s.cost")).alias("cost_count"),
        F.max(F.col("s.shipped_date")).alias("last_shipped_date"),
    )
    .orderBy(F.col("cost_count").desc())
)
