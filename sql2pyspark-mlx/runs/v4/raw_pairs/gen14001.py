from pyspark.sql import functions as F

valid = (
    df_payments
    .filter(F.to_date("paid_at") < "2025-05-24")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"),
    )
)

joined = (
    valid
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select(
        "e.*",
        F.col("d.amount").alias("order_amount"),
        F.col("d.order_date"),
        F.col("d.status").alias("order_status"),
    )
)

enriched = (
    joined
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select(
        "src.*",
        F.col("o.discount"),
        F.col("o.order_date").alias("order_order_date"),
        F.col("o.status").alias("order_status_2"),
    )
)

result = (
    enriched
    .select(
        "id",
        "method",
        "status",
        "days_since_paid_at",
        "order_amount",
        "order_date",
        "order_status",
        "discount",
        "order_order_date",
        F.when(F.col("status").rlike("^(FAILED|SUCCESS)$"), "OTHER_ZONE").when(F.col("status").rlike("^(REFUNDED)$"), "GROUP_A").otherwise("PRIORITY").alias("status_zone"),
    )
)
