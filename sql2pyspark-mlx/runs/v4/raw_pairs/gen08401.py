from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("status").isin(["IN_TRANSIT", "RETURNED"]))
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "LOST", F.col("cost")).otherwise(0)).alias("lost_cost"),
    )
    .filter(F.col("lost_cost") >= 65)
)
