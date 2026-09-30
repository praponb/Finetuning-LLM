from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left")
    .groupBy(F.col("s.id"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.sort_array(F.collect_set(F.col("o.status"))).alias("status_list"),
    )
    .orderBy(F.col("order_count").desc(), F.col("id").desc())
    .limit(5)
)
