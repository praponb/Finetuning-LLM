from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("obs"), F.col("s.id") == F.col("obs.store_id"), "inner")
    .select(F.col("s.id"), F.col("s.sqft"), F.col("obs.status_values"))
)
