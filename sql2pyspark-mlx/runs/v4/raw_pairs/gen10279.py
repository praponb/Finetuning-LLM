from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "left")
    .filter(F.col("c.signup_date") < "2024-10-21")
    .groupBy(F.col("c.id"), F.col("c.segment"))
    .agg(F.count(F.col("a.id")).alias("account_count"))
    .orderBy("segment")
)
