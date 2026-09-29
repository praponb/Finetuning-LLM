from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .groupBy(F.col("customers.segment"))
    .agg(F.countDistinct(F.col("accounts.balance")).alias("unique_customers"))
    .filter(F.col("unique_customers") >= 5)
    .orderBy(F.col("segment").desc(), "unique_customers")
    .limit(10)
)
