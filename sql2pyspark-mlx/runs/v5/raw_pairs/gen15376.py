from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter((F.col("status") == "LOST") & (F.col("status") == "IN_TRANSIT") & F.col("status").isin(["IN_TRANSIT", "LOST"]))
)

status_summary = (
    valid_shipments
    .groupBy("status")
    .agg(
        F.sum("cost").alias("total_cost"),
        F.count("cost").alias("cost_count"),
    )
)

result = (
    status_summary
    .filter(F.col("total_cost") > 270)
    .orderBy("cost_count", "status")
    .limit(1)
)
