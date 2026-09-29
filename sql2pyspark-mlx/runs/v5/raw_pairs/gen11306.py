from pyspark.sql import functions as F

result = (
    df_shipments
    .withColumn("cost_tier", F.when(F.col("cost") >= 95, "PREMIUM").otherwise("STANDARD"))
    .cube("order_id", "cost_tier")
    .agg(
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("status") == "IN_TRANSIT", 1)), F.lit(0)), 2).alias("cost_per_in_transit"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_count"),
        F.count(F.when(F.col("carrier") == "UPS", 1)).alias("ups_rows"),
        F.stddev_samp("cost").alias("cost_stddev"),
        F.grouping("order_id").alias("grouping_level"),
    )
    .orderBy(F.col("order_id").desc())
)
