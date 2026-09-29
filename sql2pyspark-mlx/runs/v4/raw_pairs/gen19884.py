from pyspark.sql import functions as F

kept_keys = (
    df_payments
    .select("order_id", "status")
    .intersectAll(
        df_payments
        .filter(F.col("order_id").isNull())
        .select("order_id", "status")
    )
)

reconciled = (
    df_payments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        F.when(F.col("status").rlike("^(SUCCESS|REFUNDED)$"), "GROUP_A").otherwise("CORE").alias("status_zone"),
    )
    .orderBy(F.col("status").desc())
)
