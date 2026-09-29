from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "OPEN", "FROZEN"]) & F.col("status").isin(["CLOSED", "OPEN", "FROZEN"]) & (F.col("account_type") == "LOAN"))
    .groupBy(
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
        "account_type",
    )
    .agg(F.count("*").alias("row_count"))
)
