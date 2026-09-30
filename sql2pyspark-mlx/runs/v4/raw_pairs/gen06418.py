from pyspark.sql import functions as F

valid = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .select(
        "*",
        F.lower("status").alias("status_lower"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_clean"),
    )
)

result = (
    valid
    .select("status", "order_id")
    .intersectAll(
        df_shipments
        .filter(F.col("order_id").isNotNull())
        .select("status", "order_id")
    )
)
