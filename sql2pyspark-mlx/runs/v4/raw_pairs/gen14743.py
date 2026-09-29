from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(F.col("c.is_active"), F.col("c.credit_limit"), F.col("cs.standard_count"))
    .orderBy(F.col("standard_count").desc(), F.col("credit_limit").desc())
)
