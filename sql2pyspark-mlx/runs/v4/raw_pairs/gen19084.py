from pyspark.sql import functions as F

src = df_accounts.filter(F.col("opened_date") > "2023-06-07")

summary = (
    src
    .groupBy("account_type", F.month("opened_date").alias("opened_month"))
    .agg(
        F.count("balance").alias("balance_count"),
        F.sum("balance").alias("total_balance"),
    )
)

result = (
    summary
    .select("account_type", "opened_month", "total_balance")
    .orderBy(F.col("total_balance").desc())
)
