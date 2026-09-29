from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.city").isin(["Singapore", "Tokyo"]))
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("c.credit_limit"),
        F.col("c.age"),
        F.col("c.country"),
    )
)
