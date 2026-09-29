from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CHECKING", "CREDIT"]) & (F.col("opened_date") >= "2023-10-01"))
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
        F.year("opened_date").alias("opened_year"),
    )
    .agg(F.avg("balance").alias("avg_balance"))
    .filter(F.col("avg_balance") > 19000)
    .orderBy(F.col("opened_year").desc())
)
