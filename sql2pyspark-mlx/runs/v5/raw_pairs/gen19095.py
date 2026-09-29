from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.sum("balance").alias("total_balance"),
        F.count("balance").alias("balance_count"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
    .orderBy("total_balance", "opened_month")
    .limit(50)
)
