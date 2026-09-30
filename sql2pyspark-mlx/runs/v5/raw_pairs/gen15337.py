from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Chiang Mai") & (F.col("budget") <= 1891000))
    .groupBy("location", "dept_name")
    .agg(
        F.sum(F.when(F.col("location") == "London", 1).otherwise(0)).alias("london_count"),
        F.min("budget").alias("min_budget"),
        F.max("budget").alias("max_budget"),
    )
    .orderBy("dept_name", "min_budget", "location")
    .limit(50)
)
