from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.max("balance").alias("max_balance"),
        F.min("balance").alias("min_balance"),
    )
    .filter(F.col("min_balance") < 38000)
    .drop("min_balance")
)
