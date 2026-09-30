from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .filter(F.col("discount") > 0.14)
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("channel") == "partner", F.col("shipping_fee")).otherwise(0)).alias("partner_shipping_fee"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.partner_shipping_fee") >= 55)
    .select(
        F.col("c.id"),
        F.col("c.credit_limit"),
        F.col("c.country"),
        F.col("cs.partner_shipping_fee"),
    )
    .orderBy("partner_shipping_fee")
)
