from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter(~F.col("status").isin(["FROZEN", "CLOSED"]) | (F.col("balance") >= 55000))
    .withColumn("balance_adjusted", F.round((F.col("balance") * 1.15), 2))
)

opened_month_summary = (
    valid_accounts
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.countDistinct("balance_adjusted").alias("unique_statuss"),
    )
)

result = (
    opened_month_summary
    .filter(F.col("unique_statuss") > 4)
    .select("opened_month", "latest_opened_date", "unique_statuss")
    .orderBy(F.col("opened_month").desc())
)
