from pyspark.sql import functions as F

order_summary = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_summary.alias("order_summary"), F.col("orders.id") == F.col("order_summary.order_id"), "left")
    .filter(F.col("order_summary.min_amount") > 1470)
    .select(
        F.col("orders.id"),
        F.col("orders.shipping_fee"),
        F.col("order_summary.method_values"),
        F.col("order_summary.min_amount"),
    )
    .orderBy(F.col("shipping_fee").desc(), F.col("id").desc())
)
