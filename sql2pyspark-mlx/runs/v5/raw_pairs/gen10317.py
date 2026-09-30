from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(F.col("a.id"), F.col("a.status"), F.col("c.segment"), F.col("c.age"))
    .orderBy("id")
    .limit(10)
)
