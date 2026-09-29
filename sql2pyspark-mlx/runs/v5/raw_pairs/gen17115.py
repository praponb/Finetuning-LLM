from pyspark.sql import functions as F

unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.min("unit_price").alias("min_order_item_unit_price"))

base = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-10-03 14:00:00").cast("timestamp"))
    .withColumn("paid_at_day", F.col("paid_at").cast("date"))
    .withColumn("method_filled", F.coalesce("method", F.lit("OTHER")))
)

enriched = (
    base
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "e.*",
        F.col("d.status").alias("order_status"),
        F.col("d.discount"),
        F.col("d.amount").alias("order_amount"),
    )
)

filtered = (
    enriched
    .filter((F.col("paid_at") >= F.lit("2023-05-29").cast("date")) & F.col("method").isNotNull())
    .withColumn("status_order_status_key", F.concat_ws("/", "status", "order_status"))
    .withColumn("resolved_method", F.coalesce("method", F.lit("UNKNOWN")))
)

enriched_payments = (
    filtered
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.amount",
        "a.paid_at_day",
        "a.method_filled",
        "a.order_status",
        "a.discount",
        "a.order_amount",
        F.col("b.order_date"),
    )
)

result = (
    enriched_payments
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "amount",
        "order_status",
        "order_date",
        F.when(F.col("order_status").rlike("^(CANCELLED|PENDING)$"), "CORE").when(F.col("order_status").rlike("^(COMPLETED|SHIPPED)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("order_status_zone"),
        F.col("min_order_item_unit_price"),
    )
)
