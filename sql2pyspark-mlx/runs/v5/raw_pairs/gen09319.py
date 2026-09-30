from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.opened_date").between("2024-12-01", "2025-06-22"))
    .groupBy(F.year(F.col("a.opened_date")).alias("opened_year"))
    .agg(F.avg(F.col("t.amount")).alias("avg_amount"))
    .orderBy(F.col("avg_amount").desc(), "opened_year")
    .limit(1)
)
