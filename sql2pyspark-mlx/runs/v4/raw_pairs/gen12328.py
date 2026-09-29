from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "left_anti")
    .filter(F.length(F.col("c.name")) > 7)
    .select(F.col("c.id"), F.col("c.age"), F.col("c.city"))
    .orderBy(F.col("city").desc(), "id")
)
