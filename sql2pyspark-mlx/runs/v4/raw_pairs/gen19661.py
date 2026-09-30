from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("account_type").isin(["CHECKING", "CREDIT", "LOAN"]) | (F.col("opened_date") > "2023-09-01")))
)

summary = (
    valid_accounts
    .groupBy(F.quarter("opened_date").alias("opened_quarter"), "account_type")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_closed"),
    )
)

result = (
    summary
    .filter(F.col("pct_closed") > 40)
    .select("opened_quarter", "account_type")
    .orderBy(F.col("opened_quarter").desc(), "account_type")
)
