from pyspark.sql import functions as F

valid = (
    df_shipments
    .filter(~F.col("carrier").eqNullSafe(F.lit("Flash")))
    .withColumn("carrier_length", F.length("carrier"))
    .withColumn("days_shipped_date_to_delivered_date", F.datediff("delivered_date", "shipped_date"))
)

result = (
    valid
    .groupBy(
        "order_id",
        F.date_trunc("YEAR", "shipped_date").alias("year_start"),
        "carrier",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("year_start").desc())
)
