from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["Singapore", "London", "Chiang Mai"]) & (F.col("location") != "Chiang Mai"))
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3922000, "high").when(F.col("budget") >= 1252000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.sum(F.when(F.col("dept_name") == "Legal", 1).otherwise(0)).alias("legal_count"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
    .filter(F.col("legal_count") > 29)
    .orderBy(F.col("avg_budget").desc(), "location", "budget_bucket")
    .limit(3)
)
