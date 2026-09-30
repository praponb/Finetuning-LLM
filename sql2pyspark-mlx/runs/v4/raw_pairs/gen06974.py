from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(~F.col("c.segment").isin(["Consumer", "Corporate", "SMB"]) & (F.col("c.city") != "New York"))
    .select(F.col("c.email"), F.col("c.credit_limit"), F.col("s.end_date"))
    .orderBy("email")
)
