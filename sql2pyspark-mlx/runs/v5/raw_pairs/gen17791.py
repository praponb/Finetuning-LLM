from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(~F.col("c.is_active"))
    .groupBy(F.col("c.city"))
    .agg(
        F.count(F.col("t.amount")).alias("amount_count"),
        F.countDistinct(F.col("t.amount")).alias("unique_merchants"),
    )
    .orderBy(F.col("unique_merchants").desc(), "city")
    .limit(1)
)
