from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("c.signup_date") >= "2024-12-09")
    .groupBy(F.col("c.city"))
    .agg(
        F.count(F.when(F.col("t.merchant") == "Lazada", 1)).alias("lazada_count"),
        F.avg(F.col("t.amount")).alias("avg_amount"),
    )
    .filter(F.col("avg_amount") > 3900)
    .orderBy(F.col("avg_amount").desc(), "city")
    .limit(3)
)
