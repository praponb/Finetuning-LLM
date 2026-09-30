from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") > "2023-09-02") & F.col("account_type").isin(["CREDIT", "SAVINGS"]))
    .groupBy("account_type")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "CHECKING", 1).otherwise(0))) / F.count("*")), 1).alias("pct_checking"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.round(((100 * F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_savings"),
    )
)
