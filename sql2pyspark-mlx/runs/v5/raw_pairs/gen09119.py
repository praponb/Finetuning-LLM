from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter(F.col("shipped_date").between("2023-05-01", "2025-07-02"))
    .select(
        "*",
        F.upper("carrier").alias("carrier_upper"),
        F.coalesce("carrier", F.lit("NONE")).alias("resolved_carrier"),
    )
)

result = (
    filtered
    .select("order_id", "status")
    .intersect(
        df_shipments
        .filter(F.col("cost") <= 84)
        .select("order_id", "status")
    )
)
