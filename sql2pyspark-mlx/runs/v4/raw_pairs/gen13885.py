from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter(~F.col("status").isin(["IN_TRANSIT", "LOST", "DELIVERED"]) & (F.col("cost") > 76))
)

shipments_stats = (
    recent_shipments
    .groupBy("carrier")
    .agg(
        F.sum(F.when(F.col("status") == "RETURNED", F.col("cost")).otherwise(0)).alias("returned_cost"),
    )
)

result = (
    shipments_stats
    .filter(F.col("returned_cost") < 54)
    .select("carrier", "returned_cost")
)
