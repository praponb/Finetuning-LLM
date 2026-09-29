from pyspark.sql import functions as F

kept_keys = (
    df_shipments
    .select("status", "id")
    .intersect(
        df_shipments
        .filter(F.col("order_id") == 22)
        .select("status", "id")
    )
)

verified = (
    df_shipments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "order_id",
        "shipped_date",
        "delivered_date",
        "status",
        F.when(F.col("status").rlike("^(RETURNED|IN_TRANSIT)$"), "PRIORITY").otherwise("EXTENDED").alias("status_zone"),
    )
    .orderBy(F.col("status_zone").desc_nulls_last())
)
