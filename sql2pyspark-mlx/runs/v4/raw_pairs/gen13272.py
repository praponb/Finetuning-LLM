from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "left")
    .groupBy(F.col("o.channel"))
    .agg(
        F.count(F.when(F.col("s.carrier") == "UPS", 1)).alias("ups_count"),
        F.min(F.col("s.cost")).alias("min_cost"),
    )
    .orderBy(F.col("min_cost").desc(), F.col("channel").desc())
)
