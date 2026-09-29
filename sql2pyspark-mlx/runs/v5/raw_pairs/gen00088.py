from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.filter(F.col("city") == "Berlin").alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("a.customer_id").isin([19, 21, 25]))
    .select(F.col("a.id"), F.col("a.balance"))
)
