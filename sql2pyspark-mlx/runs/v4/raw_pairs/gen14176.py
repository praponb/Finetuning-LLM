from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.filter(F.col("opened_date") <= "2025-01-01").alias("a"), F.col("c.id") == F.col("a.customer_id"), "left_semi")
    .filter(F.col("c.is_active"))
    .select(F.col("c.id"), F.col("c.country"))
)
