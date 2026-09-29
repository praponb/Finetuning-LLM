from pyspark.sql import functions as F

filtered_accounts = df_accounts.filter(F.col("customer_id").isin([15, 22, 28, 31]))

accounts_stats = (
    filtered_accounts
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_count"))
)

result = (
    accounts_stats
    .filter(F.col("credit_count") < 7)
    .orderBy("credit_count")
)
