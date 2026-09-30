from pyspark.sql import functions as F

amount_by_order_id = df_payments.groupBy("order_id").agg(F.avg("amount").alias("avg_payment_amount"))

matched_keys = (
    df_shipments
    .select("status", "carrier")
    .intersect(
        df_shipments
        .filter(F.col("status") != "DELIVERED")
        .select("status", "carrier")
    )
)

kept_rows = (
    df_shipments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.when(F.col("status").rlike("^(LOST|DELIVERED)$"), "EXTENDED").otherwise("GROUP_A").alias("status_zone"),
        F.col("avg_payment_amount"),
    )
    .orderBy(F.col("order_id").desc())
)
