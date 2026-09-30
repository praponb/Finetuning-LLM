from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([14, 19, 24, 29]) & (F.col("status") == "IN_TRANSIT"))
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        F.last_day("shipped_date").alias("shipped_date_month_end"),
        F.length("carrier").alias("carrier_length"),
    )
)
