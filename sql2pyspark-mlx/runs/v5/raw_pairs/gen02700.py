from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_count"),
        F.sum(F.when(F.col("account_type") == "CREDIT", F.col("balance")).otherwise(0)).alias("credit_balance"),
        F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0)).alias("closed_count"),
    )
    .filter(F.col("closed_count") > 34)
    .drop("closed_count")
    .orderBy("credit_count", F.col("credit_balance").desc())
)
