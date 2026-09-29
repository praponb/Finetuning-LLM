from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
        F.max("amount").alias("max_amount"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.cancelled_count") > 8)
    .select(
        F.col("c.id"),
        F.col("c.name"),
        F.col("cs.cancelled_count"),
        F.col("cs.max_amount"),
    )
    .orderBy(F.col("max_amount").desc())
)
