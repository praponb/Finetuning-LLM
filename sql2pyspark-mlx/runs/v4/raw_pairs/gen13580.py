from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter(F.col("c.segment") == "SMB")
    .select(F.col("o.id"), F.col("o.amount"), F.col("c.is_active"))
    .orderBy("id", F.col("amount").desc())
    .limit(5)
)
