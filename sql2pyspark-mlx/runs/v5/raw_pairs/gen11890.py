from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        F.lit("shipment").alias("origin"),
        F.col("delivered_date").alias("event_date"),
    )
    .unionByName(
        df_stores
        .select(F.lit("store").alias("origin"), F.col("opened_date").alias("event_date"))
    )
    .distinct()
)
