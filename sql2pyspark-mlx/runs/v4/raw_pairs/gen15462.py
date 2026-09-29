from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.countDistinct("balance").alias("unique_account_types"),
        F.min("balance").alias("min_balance"),
    )
    .orderBy("min_balance", "customer_id")
    .limit(50)
)
