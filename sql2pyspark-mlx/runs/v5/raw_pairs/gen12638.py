from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("customer_id") == 18) & F.col("status").isNotNull())
    .groupBy(F.quarter("opened_date").alias("opened_quarter"), "account_type")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.count("*").alias("cnt"),
    )
    .orderBy("opened_quarter")
)
