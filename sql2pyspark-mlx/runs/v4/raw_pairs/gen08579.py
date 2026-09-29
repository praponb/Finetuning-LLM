from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
        "customer_id",
    )
    .agg(F.max("balance").alias("max_balance"))
    .filter(F.col("max_balance") < 33000)
)
