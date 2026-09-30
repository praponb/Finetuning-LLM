from pyspark.sql import functions as F

recent_shipments = df_shipments.filter(F.col("delivered_date") < "2025-09-23")

shipments_stats = (
    recent_shipments
    .groupBy("carrier")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
        F.round(((100 * F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_returned"),
    )
)

result = (
    shipments_stats
    .orderBy(F.col("pct_returned").desc(), "carrier")
    .limit(10)
)
