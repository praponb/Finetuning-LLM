from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

exploded = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("value"))

result = (
    exploded
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", F.col("avg_account_balance"))
    .orderBy(F.col("avg_account_balance").desc(), F.col("id").desc())
)
