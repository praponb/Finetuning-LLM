from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .groupBy("customer_id")
    .agg(F.max("balance").alias("max_balance"))
)

result = (
    df_customers.alias("c")
    .join(accounts_by_customer.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.max_balance") > 62000)
    .select(F.col("c.id"), F.col("c.age"), F.col("t.max_balance"))
    .orderBy("id")
    .limit(10)
)
