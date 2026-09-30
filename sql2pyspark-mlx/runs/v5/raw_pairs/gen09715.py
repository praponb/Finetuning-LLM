from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

kept = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.quantity") <= 5), "left_anti")
)

result = (
    kept
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        F.when(F.col("status").rlike("^(SHIPPED|RETURNED)$"), "PRIORITY").otherwise("GROUP_B").alias("status_zone"),
        F.col("sum_account_balance"),
    )
    .orderBy(F.col("status_zone").desc())
)
