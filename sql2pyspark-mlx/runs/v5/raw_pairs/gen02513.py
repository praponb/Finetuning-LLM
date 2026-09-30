from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") <= 74) & F.col("shipped_date").between("2023-08-01", "2025-05-01") & (F.col("cost") < 35))
    .groupBy(
        "status",
        "carrier",
        F.when(F.col("cost") >= 93, "high").when(F.col("cost") >= 49, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.countDistinct("cost").alias("unique_statuss"),
        F.avg("cost").alias("avg_cost"),
    )
    .filter(F.col("cost_range") < 68)
)
