from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.sum("budget").alias("total_budget"),
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count"),
    )
    .orderBy("hr_count", "location")
    .limit(50)
)
