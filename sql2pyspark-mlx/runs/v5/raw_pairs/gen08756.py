from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.city"))
    .agg(F.countDistinct(F.col("t.amount")).alias("unique_merchants"))
    .orderBy(F.col("unique_merchants").desc(), "city")
)
