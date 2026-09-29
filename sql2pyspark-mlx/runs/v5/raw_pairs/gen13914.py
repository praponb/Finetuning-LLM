from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .groupBy(F.month(F.col("o.order_date")).alias("order_month"), F.col("o.channel"))
    .agg(
        F.countDistinct(F.col("s.cost")).alias("unique_orders"),
        F.max(F.col("s.cost")).alias("max_cost"),
    )
    .orderBy(F.col("channel").desc(), "order_month")
    .limit(10)
)
