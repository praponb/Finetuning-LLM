from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

exploded = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("value"))
)

result = (
    exploded
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select("customer_id", F.col("max_account_balance"))
    .orderBy(F.col("customer_id").desc())
)
