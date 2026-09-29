from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([27, 29, 30, 40]))
    .groupBy("order_id", "status")
    .agg(
        F.sum(F.when(F.col("carrier") == "UPS", 1).otherwise(0)).alias("ups_count"),
        F.count("*").alias("shipment_count"),
    )
    .filter(F.col("ups_count") > 67)
    .orderBy("status")
)
