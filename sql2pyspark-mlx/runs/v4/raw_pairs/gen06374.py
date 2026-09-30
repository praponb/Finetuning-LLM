from pyspark.sql import functions as F

src = (
    df_accounts
    .filter((F.col("status") == "FROZEN") & F.col("balance").between(49000, 71000) & ~F.col("account_type").isin(["CHECKING", "SAVINGS", "LOAN"]))
)

summary = (
    src
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(F.count("balance").alias("balance_count"))
)

result = (
    summary
    .filter(F.col("balance_count") >= 7)
    .select("opened_month", "balance_count")
)
