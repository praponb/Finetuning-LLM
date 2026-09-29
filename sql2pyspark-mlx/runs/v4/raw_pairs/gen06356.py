from pyspark.sql import functions as F

matched_keys = (
    df_orders
    .select("id", "customer_id")
    .exceptAll(
        df_orders
        .filter(F.col("store_id").isNotNull())
        .select("id", "customer_id")
    )
)

kept_rows = (
    df_orders
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "customer_id",
        "store_id",
        "status",
        "discount",
        "shipping_fee",
        F.when(F.col("status").rlike("^(CANCELLED|SHIPPED)$"), "PRIORITY").when(F.col("status").rlike("^(COMPLETED|PENDING)$"), "OTHER_ZONE").otherwise("CORE").alias("status_zone"),
    )
    .orderBy(F.col("customer_id").asc_nulls_last())
)
