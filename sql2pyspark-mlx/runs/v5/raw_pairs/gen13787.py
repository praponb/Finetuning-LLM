from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .groupBy("account_type", F.month("opened_date").alias("opened_month"))
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.sum(F.when(F.col("account_type") == "CHECKING", 1).otherwise(0)).alias("checking_count"),
    )
)
