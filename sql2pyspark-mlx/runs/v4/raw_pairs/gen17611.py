from pyspark.sql import functions as F

filtered_shipments = (
    df_shipments
    .filter(F.col("order_id").isin([8, 19]) & F.col("cost").between(45, 92))
    .withColumn("carrier_status_key", F.concat_ws(",", "carrier", "status"))
)

status_summary = (
    filtered_shipments
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_in_transit"),
        F.count("cost").alias("cost_count"),
    )
)

result = (
    status_summary
    .orderBy("status")
    .limit(5)
)
