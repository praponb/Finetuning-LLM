from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") == "Flash") & ((F.col("status") == "IN_TRANSIT") | (F.year("delivered_date") == 2024)))
    .groupBy(
        F.when(F.col("carrier").isin(["UPS", "DHL"]), "tier_1").otherwise("other").alias("carrier_group"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "IN_TRANSIT", F.col("cost")).otherwise(0)).alias("in_transit_cost"),
        F.min("delivered_date").alias("min_delivered_date"),
    )
)
