from pyspark.sql import functions as F

valid = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-07-04").cast("date")) & F.col("order_id").isNotNull())
    .withColumn("shipped_date_plus_months", F.add_months("shipped_date", 1))
    .withColumn("status_lower", F.lower("status"))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("DIRECT")))
)

result = (
    valid
    .select("status")
    .subtract(
        df_shipments
        .filter(F.col("delivered_date") < "2025-07-01")
        .select("status")
    )
)
