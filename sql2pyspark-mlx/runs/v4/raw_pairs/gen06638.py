from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("a.status") == "OPEN")
    .groupBy(F.col("c.id"), F.col("c.city"))
    .agg(F.count(F.col("a.id")).alias("account_count"))
    .orderBy("id", F.col("account_count").desc(), "city")
    .limit(3)
)
