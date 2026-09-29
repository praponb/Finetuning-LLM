from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.segment").isin(["SMB", "Corporate", "Consumer"]) & (F.col("s.status") == "CANCELLED"))
    .select(F.col("s.id"), F.col("s.status"), F.col("s.start_date"), F.col("c.country"))
    .orderBy(F.col("id").desc())
)
