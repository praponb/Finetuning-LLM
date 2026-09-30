from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.store_id").isNotNull())
    .groupBy(
        F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"),
        F.col("c.country"),
    )
    .agg(
        F.sum(F.when(F.col("o.status") == "PENDING", F.col("o.amount")).otherwise(0)).alias("pending_amount"),
        (F.max(F.col("o.amount")) - F.min(F.col("o.amount"))).alias("amount_range"),
    )
    .orderBy("signup_month", F.col("pending_amount").desc())
)
