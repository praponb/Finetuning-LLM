from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "left")
    .filter(F.col("s.status") == "RETURNED")
    .groupBy(F.col("o.status"))
    .agg(
        F.sum(F.col("s.cost")).alias("total_cost"),
        F.sum(F.when(F.col("s.carrier") == "Flash", 1).otherwise(0)).alias("flash_count"),
    )
)
