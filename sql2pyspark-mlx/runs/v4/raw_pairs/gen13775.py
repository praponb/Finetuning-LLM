from pyspark.sql import functions as F

remaining_keys = (
    df_payments
    .select("method")
    .subtract(
        df_payments
        .filter(F.col("order_id").isNull())
        .select("method")
    )
)

kept_rows = (
    df_payments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.method") == F.col("k.method")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.when(F.col("status").rlike("^(SUCCESS|REFUNDED)$"), "GROUP_A").when(F.col("status").rlike("^(FAILED)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("status_zone"),
    )
    .orderBy("order_id")
)
