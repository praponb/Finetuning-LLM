from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "CHECKING", 1).otherwise(0))) / F.count("*")), 1).alias("pct_checking"),
    )
    .orderBy("status")
)
