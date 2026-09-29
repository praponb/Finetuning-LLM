from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.min("delivered_date").alias("min_delivered_date"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.max("delivered_date").alias("last_delivered_date"),
    )
    .filter(F.col("cost_range") > 56)
)
