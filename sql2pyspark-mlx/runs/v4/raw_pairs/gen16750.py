from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.country"), F.col("c.is_active"))
    .agg(
        F.sum(F.col("t.amount")).alias("total_amount"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("total_amount") > 13000)
    .orderBy("country")
)
