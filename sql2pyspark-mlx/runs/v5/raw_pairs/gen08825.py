from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .groupBy("account_type", F.month("opened_date").alias("opened_month"))
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.avg("balance").alias("avg_balance"),
    )
    .orderBy(F.col("account_type").desc(), "opened_month")
)
