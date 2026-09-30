from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter(F.col("status") != "OPEN")
    .select("*", F.last_day("opened_date").alias("opened_date_month_end"))
)

result = (
    valid
    .groupBy(
        F.when(F.col("balance") >= 73000, "LARGE").otherwise("SMALL").alias("balance_tier"),
        "account_type",
    )
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_rows"),
    )
)
