from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .filter(F.col("c.is_active") & (F.col("c.age") < 46))
    .select(F.col("a.status"), F.col("c.country"), F.col("c.email"))
)
