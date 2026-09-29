from pyspark.sql import functions as F

recent_accounts = df_accounts.filter(F.col("opened_date") > "2023-07-30")

accounts_stats = (
    recent_accounts
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
        F.sum("balance").alias("total_balance"),
    )
)

result = (
    accounts_stats
    .orderBy("total_balance", "opened_cohort")
    .limit(5)
)
