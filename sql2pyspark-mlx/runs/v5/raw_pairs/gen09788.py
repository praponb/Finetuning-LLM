from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter(F.col("shipped_date").between("2023-03-06", "2024-11-01"))
    .select("*", F.date_sub("shipped_date", 78).alias("shipped_date_minus_78d"))
)

result = (
    filtered
    .groupBy(
        F.date_trunc("YEAR", "shipped_date_minus_78d").alias("year_start"),
        "carrier",
        F.when(F.col("cost") >= 89, "HIGH").otherwise("LOW").alias("cost_tier"),
    )
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.round(F.avg("cost"), 2).alias("avg_cost_2"),
        F.count_if(F.col("carrier") == "Flash").alias("flash_events"),
        F.count_if(F.col("status") == "IN_TRANSIT").alias("in_transit_events"),
    )
    .filter(F.col("flash_events") > 1)
    .orderBy(F.col("in_transit_events").desc_nulls_last())
)
