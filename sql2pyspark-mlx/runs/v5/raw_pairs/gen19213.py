from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "v"),
    )
)

result = (
    lines
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", "created_at", "key", "v", F.col("min_account_balance"))
)
