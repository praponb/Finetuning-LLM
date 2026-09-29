from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") <= 36)
    .groupBy("status", F.month("delivered_date").alias("delivered_month"))
    .agg(
        F.count("*").alias("shipment_count"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
    .filter(F.col("shipment_count") >= 6)
    .select("status", "delivered_month", "shipment_count", "carrier_list")
)
