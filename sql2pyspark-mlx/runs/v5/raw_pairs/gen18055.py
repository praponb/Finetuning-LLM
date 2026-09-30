from pyspark.sql import functions as F

excluded_customers = df_customers.filter(F.month("signup_date").isin([7, 8, 12])).select("id")

result = (
    df_subscriptions.alias("s")
    .join(excluded_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("s.customer_id").isNotNull() | F.lit(excluded_customers.isEmpty()))
    .filter(F.col("s.status").isin(["ACTIVE", "CANCELLED", "EXPIRED"]))
    .select(F.col("s.id"), F.col("s.start_date"), F.col("s.end_date"))
)
