from pyspark.sql import functions as F

filtered_accounts = df_accounts.filter(F.col("customer_id").isNull())

status_summary = (
    filtered_accounts
    .groupBy("status")
    .agg(F.min("opened_date").alias("min_opened_date"))
)

result = status_summary.orderBy(F.col("status").desc(), "min_opened_date")
