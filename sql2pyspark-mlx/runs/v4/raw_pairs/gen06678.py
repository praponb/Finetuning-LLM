from pyspark.sql import functions as F

amount_by_order_id = df_payments.groupBy("order_id").agg(F.sum("amount").alias("sum_payment_amount"))

matched_keys = (
    df_payments
    .select("method", "status")
    .exceptAll(
        df_payments
        .filter(F.hour("paid_at").between(8, 14))
        .select("method", "status")
    )
)

kept_rows = (
    df_payments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.method") == F.col("k.method")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.when(F.col("method").rlike("^(card|bank_transfer)$"), "OTHER_ZONE").when(F.col("method").rlike("^(paypal|cash)$"), "GROUP_B").otherwise("GROUP_A").alias("method_zone"),
        F.col("sum_payment_amount"),
    )
)
