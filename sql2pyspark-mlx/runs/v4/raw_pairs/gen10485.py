from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .filter(F.year("delivered_date") == 2024)
    .groupBy("order_id")
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .filter(F.col("os.stddev_cost") > 40)
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.amount"),
        F.col("os.stddev_cost"),
    )
    .orderBy("amount", F.col("order_date").desc())
)
