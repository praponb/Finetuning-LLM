from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") < "2025-10-06")
    .groupBy("account_type")
    .agg(
        F.countDistinct("balance").alias("unique_customers"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
    .filter(F.col("unique_customers") > 4)
    .orderBy("account_type")
)
