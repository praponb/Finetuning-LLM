from pyspark.sql import functions as F

result = (
    df_shipments
    .withColumn("cost_tier", F.when(F.col("cost") >= 76, "LARGE").otherwise("SMALL"))
    .withColumn("year_start", F.date_trunc("YEAR", "shipped_date"))
    .rollup("cost_tier", "year_start")
    .agg(
        F.count("*").alias("row_count"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.grouping("cost_tier").alias("gid"),
    )
    .filter(F.col("num_status") >= 1)
    .orderBy(F.col("cost_tier").desc())
)
