from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("carrier").isin(["DHL", "Kerry"]), "group_a").otherwise("other").alias("carrier_group"),
    )
    .agg(
        F.max("shipped_date").alias("latest_shipped_date"),
        F.max("delivered_date").alias("latest_delivered_date"),
        F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_count"),
    )
)
