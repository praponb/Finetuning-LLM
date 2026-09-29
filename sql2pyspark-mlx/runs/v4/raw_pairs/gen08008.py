from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") != "SAVINGS") | (F.col("account_type") == "CHECKING"))
    .groupBy("account_type")
    .agg(
        F.min("opened_date").alias("first_opened_date"),
        F.round(((100 * F.sum(F.when(F.col("account_type") == "CHECKING", 1).otherwise(0))) / F.count("*")), 1).alias("pct_checking"),
    )
    .filter(F.col("pct_checking") > 45)
    .orderBy(F.col("account_type").desc(), "first_opened_date")
)
