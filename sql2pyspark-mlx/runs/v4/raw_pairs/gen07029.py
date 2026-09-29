from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.when(F.col("cost") >= 85, "LARGE").otherwise("SMALL").alias("cost_tier"))
    .agg(
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_rows"),
        F.sum(F.when(F.col("carrier") == "DHL", F.col("cost")).otherwise(0)).alias("dhl_cost"),
        F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_rows"),
    )
)
