from pyspark.sql import functions as F

valid = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .select(
        "*",
        F.lower("status").alias("status_lower"),
        F.last_day("shipped_date").alias("shipped_date_month_end"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_clean"),
    )
)

result = (
    valid
    .select("carrier")
    .exceptAll(
        df_shipments
        .filter(F.col("order_id").isin([8, 18, 25]))
        .select("carrier")
    )
)
