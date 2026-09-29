from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("a.opened_date").between("2023-09-01", "2025-03-01"))
    .groupBy(F.quarter(F.col("a.opened_date")).alias("opened_quarter"))
    .agg(
        F.max(F.col("t.amount")).alias("max_amount"),
        F.min(F.col("t.amount")).alias("min_amount"),
    )
    .filter(F.col("max_amount") >= 3100)
    .orderBy("max_amount")
)
