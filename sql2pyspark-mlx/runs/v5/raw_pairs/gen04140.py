from pyspark.sql import functions as F

amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("amount").alias("avg_order_amount"))

remaining_keys = (
    df_subscriptions
    .select("customer_id", "id")
    .exceptAll(
        df_subscriptions
        .filter(F.col("monthly_fee").between(140, 160))
        .select("customer_id", "id")
    )
)

kept_rows = (
    df_subscriptions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.col("avg_order_amount"),
    )
    .orderBy(F.col("status").desc_nulls_last())
)
