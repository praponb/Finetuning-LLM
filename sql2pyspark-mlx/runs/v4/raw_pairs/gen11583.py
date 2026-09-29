from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter(F.col("customer_id") == 13)
    .withColumn("opened_date_period", F.date_trunc("YEAR", "opened_date"))
    .withColumn("abs_balance", F.abs("balance"))
    .withColumn("resolved_status", F.coalesce("status", F.lit("DIRECT")))
)

result = (
    valid
    .select("id", "account_type", "balance", "abs_balance")
    .unpivot(
        ["id", "account_type"],
        ["balance", "abs_balance"],
        "measure",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
