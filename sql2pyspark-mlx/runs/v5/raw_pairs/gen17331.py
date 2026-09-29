from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("account_type").isin(["CREDIT", "CHECKING"]) & ~F.col("account_type").isin(["LOAN", "CREDIT"]))
    .agg(F.round(F.stddev("balance"), 2).alias("stddev_balance"))
)
