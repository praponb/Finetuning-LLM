from pyspark.sql import functions as F

src = (
    df_accounts
    .filter((F.col("balance") > 73000) & (F.col("customer_id").isNotNull() | (F.col("account_type") == "LOAN")))
)

summary = (
    src
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.min("balance").alias("min_balance"),
        F.count("*").alias("row_count"),
    )
)

result = (
    summary
    .filter(F.col("row_count") < 6)
    .orderBy("opened_month")
)
