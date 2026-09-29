from pyspark.sql import functions as F

order_summary = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("method") == "paypal", 1).otherwise(0)).alias("paypal_count"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.paypal_count") > 66)
    .select(F.col("o.shipping_fee"), F.col("os.paypal_count"), F.col("os.method_list"))
)
