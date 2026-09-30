from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.datediff("delivered_date", "shipped_date").alias("days_shipped_date_to_delivered_date"),
        F.last_day("shipped_date").alias("shipped_date_month_end"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_filled"),
    )
)

result = (
    cleaned
    .select("carrier", "status")
    .exceptAll(
        df_shipments
        .filter(F.col("cost") >= 39)
        .select("carrier", "status")
    )
)
