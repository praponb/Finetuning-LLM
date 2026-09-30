from pyspark.sql import functions as F

amount_by_order_id = df_payments.groupBy("order_id").agg(F.avg("amount").alias("avg_payment_amount"))

kept_keys = (
    df_shipments
    .select("order_id")
    .exceptAll(
        df_shipments
        .filter(F.col("cost").between(49, 92))
        .select("order_id")
    )
)

reconciled = (
    df_shipments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("carrier").isin(["FedEx", "DHL"]))
    .select(
        "id",
        "order_id",
        "shipped_date",
        F.date_add("delivered_date", 57).alias("delivered_date_plus_57d"),
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
    )
)

remaining_keys = (
    base
    .select("order_id", "id")
    .intersect(
        df_shipments
        .filter(F.col("order_id").isNotNull())
        .select("order_id", "id")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .select(
        "src.id",
        "src.order_id",
        F.col("d.order_date"),
        F.col("d.shipping_fee"),
        F.col("d.status"),
    )
)

with_dims_shipments = (
    with_dims
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.discount"))
)

result = (
    with_dims_shipments
    .join(amount_by_order_id, on="order_id", how="left")
    .select("id", "order_id", "shipping_fee", "discount", F.col("avg_payment_amount"))
    .orderBy("id", "avg_payment_amount")
)
