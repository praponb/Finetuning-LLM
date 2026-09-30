from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") < 1869000) & (F.col("location") != "Chiang Mai"))
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.count(F.when(F.col("location") == "Singapore", 1)).alias("singapore_count"),
    )
    .orderBy(F.col("avg_budget").desc())
)
