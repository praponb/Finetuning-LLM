from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter((F.col("shipped_date") <= "2025-05-01") & F.col("cost").between(56, 90))
)

summary = (
    valid_shipments
    .groupBy("status", "carrier")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
        F.sum(F.when(F.col("status") == "LOST", 1).otherwise(0)).alias("lost_count"),
    )
)

result = summary.filter(F.col("lost_count") >= 58)
