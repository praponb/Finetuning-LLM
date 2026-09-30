from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter(F.col("account_type") != "CHECKING")
    .withColumn("status_lower", F.lower("status"))
)

account_type_summary = (
    filtered_accounts
    .groupBy("account_type")
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.max("opened_date").alias("last_opened_date"),
    )
)

result = (
    account_type_summary
    .select("account_type", "stddev_balance", "last_opened_date")
    .orderBy(F.col("stddev_balance").desc())
)
