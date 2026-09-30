from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-05-12", "2025-03-01"))
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("t.stddev_cost"),
        F.col("t.status_values"),
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
