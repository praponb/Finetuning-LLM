from pyspark.sql import functions as F

result = (
    df_products
    .select(F.col("launch_date").alias("event_date"), F.lit("product").alias("source"))
    .unionByName(
        df_shipments
        .select(
        F.col("shipped_date").alias("event_date"),
        F.lit("shipment").alias("source"),
    )
    )
)
