from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([3, 10, 12]))
    .groupBy("carrier", "order_id")
    .agg(
        F.countDistinct("cost").alias("unique_orders"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .filter(F.col("unique_orders") >= 4)
    .select("carrier", "order_id", "unique_orders", "status_values")
)
