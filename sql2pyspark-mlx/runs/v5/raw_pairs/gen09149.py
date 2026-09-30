from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("location").isin(["Singapore", "Berlin"]))
    .withColumn("location_filled", F.coalesce("location", F.lit("N/A")))
)

summary = (
    base
    .groupBy(
        F.when(F.col("budget") >= 3125000, "high").when(F.col("budget") >= 1996000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.count(F.when(F.col("location") == "London", 1)).alias("london_count"),
    )
)

result = (
    summary
    .filter(F.col("london_count") > 5)
    .select("budget_bucket", "budget_range", "london_count")
    .orderBy("budget_bucket")
)
