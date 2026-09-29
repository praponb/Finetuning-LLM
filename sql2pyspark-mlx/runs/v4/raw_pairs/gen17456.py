from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.filter(F.col("customer_id").isNotNull()).alias("a"), F.col("c.id") == F.col("a.customer_id"), "left_anti")
    .select(F.col("c.id"), F.col("c.country"), F.col("c.email"), F.col("c.age"))
)
