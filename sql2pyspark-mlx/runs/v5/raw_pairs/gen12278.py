from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter((~F.col("carrier").eqNullSafe(F.lit("UPS"))) & (F.col("status") == "RETURNED"))
    .withColumn("delivered_date_plus_months", F.add_months("delivered_date", 12))
    .withColumn("carrier_clean", F.coalesce("carrier", F.lit("OTHER")))
)

result = (
    filtered
    .select("carrier")
    .intersect(
        df_shipments
        .filter(F.col("order_id") == 25)
        .select("carrier")
    )
)
