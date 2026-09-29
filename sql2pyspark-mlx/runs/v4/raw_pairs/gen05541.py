from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.store_name").like("%Downtown%"))
    .groupBy(F.col("s.city"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("p.method")))).alias("method_values"),
    )
    .orderBy("city")
)
