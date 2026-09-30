from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", "coupon_codes", F.inline("items"))
)

result = (
    flattened
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "product_id",
        "price",
        "promo",
        F.col("sum_account_balance"),
    )
    .orderBy(F.col("created_at").desc())
)
