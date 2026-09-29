from pyspark.sql import functions as F

src = df_shipments.filter(F.col("shipped_date") < "2025-08-01")

status_group_summary = (
    src
    .groupBy(
        F.when(F.col("status").isin(["RETURNED", "IN_TRANSIT"]), "tier_1").otherwise("other").alias("status_group"),
    )
    .agg(F.count(F.when(F.col("carrier") == "FedEx", 1)).alias("fedex_count"))
)

result = (
    status_group_summary
    .filter(F.col("fedex_count") >= 6)
    .select("status_group", "fedex_count")
    .orderBy("fedex_count")
)
