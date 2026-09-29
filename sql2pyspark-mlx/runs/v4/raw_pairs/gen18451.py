from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .filter(F.col("channel") != "partner")
    .groupBy("customer_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.sum(F.when(F.col("status") == "RETURNED", F.col("amount")).otherwise(0)).alias("returned_amount"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(
        F.col("c.credit_limit"),
        F.col("c.country"),
        F.col("cs.amount_range"),
        F.col("cs.returned_amount"),
    )
    .orderBy("country")
)
