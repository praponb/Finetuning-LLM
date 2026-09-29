from pyspark.sql import functions as F

discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("discount").alias("sum_order_discount"))

matched_keys = (
    df_subscriptions
    .select("customer_id", "status")
    .intersect(
        df_subscriptions
        .filter(F.col("customer_id").isNull())
        .select("customer_id", "status")
    )
)

verified = (
    df_subscriptions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("status").rlike("^(ACTIVE|EXPIRED)$"), "OTHER_ZONE").when(F.col("status").rlike("^(CANCELLED|PAUSED)$"), "GROUP_B").otherwise("EXTENDED").alias("status_zone"),
        F.col("sum_order_discount"),
    )
    .orderBy(F.col("start_date").asc_nulls_first())
)
