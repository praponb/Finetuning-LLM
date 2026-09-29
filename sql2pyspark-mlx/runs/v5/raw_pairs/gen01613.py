from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.filter(F.col("account_type") == "LOAN").alias("a"), F.col("c.id") == F.col("a.customer_id"), "left_semi")
    .select(F.col("c.id"), F.col("c.name"), F.col("c.country"))
    .orderBy("country", "id")
    .limit(50)
)
