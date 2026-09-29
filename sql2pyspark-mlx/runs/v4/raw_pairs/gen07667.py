from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") >= "2025-10-01") & (F.col("status") == "IN_TRANSIT") & (F.col("carrier") != "UPS"))
    .groupBy(
        F.when(F.col("cost") >= 77, "high").when(F.col("cost") >= 44, "mid").otherwise("low").alias("cost_bucket"),
        F.when(F.year("shipped_date") < 2024, "before_2024").when(F.year("shipped_date") == 2024, "2024").otherwise("after_2024").alias("shipped_cohort"),
        "carrier",
    )
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.count("cost").alias("cost_count"),
    )
    .filter(F.col("stddev_cost") > 60)
    .orderBy("stddev_cost", "cost_bucket", "shipped_cohort", "carrier")
    .limit(3)
)
