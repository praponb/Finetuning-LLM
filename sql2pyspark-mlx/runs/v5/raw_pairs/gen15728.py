from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") >= "2025-02-03")
    .select(
        "id",
        "shipped_date",
        "carrier",
        "delivered_date",
        F.upper("carrier").alias("carrier_upper"),
        F.coalesce("carrier", F.lit("unassigned")).alias("carrier_filled"),
    )
    .orderBy("shipped_date", "delivered_date")
)
