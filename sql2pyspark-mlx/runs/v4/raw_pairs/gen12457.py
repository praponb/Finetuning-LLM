from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isNull() & (F.col("balance") >= 36000))
    .groupBy("status")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.countDistinct("balance").alias("unique_account_types"),
    )
)
