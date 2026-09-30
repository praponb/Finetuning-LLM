from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4410000, "high").when(F.col("budget") >= 940000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
        F.when(F.col("location").isin(["Tokyo", "Chiang Mai"]), "tier_1").otherwise("other").alias("location_group"),
    )
    .agg(F.count(F.when(F.col("location") == "Berlin", 1)).alias("berlin_count"))
)
