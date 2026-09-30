from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .filter(F.col("shipping_fee") >= 26)
    .groupBy("customer_id")
    .agg((F.max("discount") - F.min("discount")).alias("discount_range"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(F.col("c.id"), F.col("c.country"), F.col("cs.discount_range"))
    .orderBy(F.col("id").desc(), "country")
)
