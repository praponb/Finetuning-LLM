from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .filter(F.col("t.amount") > 3100)
    .groupBy(F.quarter(F.col("a.opened_date")).alias("opened_quarter"))
    .agg(
        F.max(F.col("t.amount")).alias("max_amount"),
        F.count("*").alias("total_count"),
    )
    .orderBy("total_count")
)
