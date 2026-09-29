from pyspark.sql import functions as F

src = (
    df_accounts
    .filter((F.col("status") != "OPEN") & F.col("customer_id").isin([10, 25]))
)

agg = (
    src
    .groupBy("account_type")
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"))
)

result = agg.filter(F.col("avg_balance") >= 56)
