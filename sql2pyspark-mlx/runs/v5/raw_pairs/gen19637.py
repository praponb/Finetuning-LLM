from pyspark.sql import functions as F

result = (
    df_products
    .select(F.col("launch_date").alias("event_date"))
    .subtract(
        df_shipments
        .filter(F.col("order_id").isNotNull())
        .select(F.col("delivered_date").alias("event_date"))
    )
)
