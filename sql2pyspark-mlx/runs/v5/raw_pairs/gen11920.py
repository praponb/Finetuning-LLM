from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

matched_keys = (
    df_orders
    .select("store_id", "customer_id")
    .subtract(
        df_orders
        .filter(F.col("discount").between(0.22, 0.27))
        .select("store_id", "customer_id")
    )
)

verified = (
    df_orders
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.col("max_account_balance"),
    )
)
