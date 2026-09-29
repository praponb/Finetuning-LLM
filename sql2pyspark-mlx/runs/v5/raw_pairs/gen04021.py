from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([9, 38, 40]))
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(F.count("balance").alias("balance_count"))
)
