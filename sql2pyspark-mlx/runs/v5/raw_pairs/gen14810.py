from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

kept_keys = (
    df_orders
    .select("channel", "customer_id")
    .intersect(
        df_orders
        .filter(F.col("channel") != "web")
        .select("channel", "customer_id")
    )
)

kept_rows = (
    df_orders
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "discount",
        F.col("sum_account_balance"),
    )
    .orderBy(F.col("discount").asc_nulls_last(), "order_date")
)
