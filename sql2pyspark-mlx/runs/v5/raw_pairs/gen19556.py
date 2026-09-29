from pyspark.sql import functions as F

kept_keys = (
    df_payments
    .select("status", "order_id")
    .subtract(
        df_payments
        .filter(~F.col("status").isin(["FAILED", "SUCCESS"]))
        .select("status", "order_id")
    )
)

verified = (
    df_payments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.when(F.col("status").rlike("^(FAILED|REFUNDED)$"), "CORE").otherwise("EXTENDED").alias("status_zone"),
    )
    .orderBy(F.col("paid_at").desc(), "status")
)
