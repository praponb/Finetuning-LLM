from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") > "2025-01-01") & (F.col("customer_id") == 38) & (F.col("balance") > 77000))
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
        "status",
    )
    .agg(F.count("*").alias("account_count"))
)
