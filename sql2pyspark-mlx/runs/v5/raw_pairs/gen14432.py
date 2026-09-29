from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter((F.col("account_type") != "CREDIT") & F.col("customer_id").isin([5, 21, 25, 28]))
)

agg = (
    recent_accounts
    .groupBy("status")
    .agg(F.countDistinct("balance").alias("unique_customers"))
)

result = agg.orderBy(F.col("unique_customers").desc())
