from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        F.col("delivered_date").alias("event_date"),
        F.lit("shipment").alias("source"),
    )
    .unionByName(
        df_orders
        .select(F.col("order_date").alias("event_date"), F.lit("order").alias("source"))
    )
)
