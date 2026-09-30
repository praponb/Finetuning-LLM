from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .filter(~F.col("channel").isin(["web", "store"]))
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("shipping_fee")).otherwise(0)).alias("cancelled_shipping_fee"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(
        F.col("c.credit_limit"),
        F.col("cs.cancelled_shipping_fee"),
        F.col("cs.channel_list"),
    )
    .orderBy("cancelled_shipping_fee")
)
