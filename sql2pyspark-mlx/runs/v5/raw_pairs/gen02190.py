from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "CLOSED", "OPEN"]) & F.col("opened_date").between("2024-01-01", "2024-04-18") & (F.col("balance") <= 20000))
    .groupBy("account_type")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "CHECKING", 1).otherwise(0))) / F.count("*")), 1).alias("pct_checking"),
    )
    .filter(F.col("pct_checking") > 61)
)
