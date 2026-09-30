from pyspark.sql import functions as F

amount_by_order_id = df_payments.groupBy("order_id").agg(F.min("amount").alias("min_payment_amount"))

remaining_keys = (
    df_payments
    .select("id", "method")
    .subtract(
        df_payments
        .filter(F.col("amount") < 1500)
        .select("id", "method")
    )
)

reconciled = (
    df_payments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.method") == F.col("k.method")), "inner")
    .select("s.*")
)

matched_keys = (
    reconciled
    .select("order_id", "method")
    .intersect(
        df_payments
        .filter(F.col("method").isNull())
        .select("order_id", "method")
    )
)

kept_rows = (
    reconciled
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.method") == F.col("k.method")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.channel"), F.col("b.status").alias("order_status"))
)

with_window = (
    joined
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & F.col("a.paid_at").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    with_window
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "order_status",
        "promo_id",
        "discount_pct",
        F.col("min_payment_amount"),
    )
    .orderBy(F.col("promo_id").asc_nulls_last())
)
