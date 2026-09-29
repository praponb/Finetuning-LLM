from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.channel").isin(["partner", "store"]) & (F.col("o.discount") > 0.33))
    .select(F.col("o.channel"), F.col("c.age"))
    .orderBy(F.col("age").desc())
)
