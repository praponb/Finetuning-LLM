from pyspark.sql import functions as F

amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("amount").alias("sum_order_amount"))

matched_keys = (
    df_subscriptions
    .select("status", "plan")
    .subtract(
        df_subscriptions
        .filter(F.col("end_date").between("2023-10-01", "2024-04-29"))
        .select("status", "plan")
    )
)

kept_rows = (
    df_subscriptions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        F.when(F.col("status").rlike("^(ACTIVE|EXPIRED)$"), "PRIORITY").otherwise("GROUP_B").alias("status_zone"),
        F.col("sum_order_amount"),
    )
    .orderBy("id")
)
