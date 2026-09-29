from pyspark.sql import functions as F

src = (
    df_accounts
    .filter((F.col("account_type") == "CREDIT") & F.col("customer_id").isin([1, 9, 37]))
)

agg = (
    src
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(F.sum("balance").alias("total_balance"))
)

result = (
    agg
    .filter(F.col("total_balance") > 331000)
    .orderBy("opened_month")
    .limit(5)
)
