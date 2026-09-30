from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 1431000)
    .groupBy("location")
    .agg(
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count"),
        F.sum(F.when(F.col("location") == "New York", F.col("budget")).otherwise(0)).alias("new_york_budget"),
    )
    .filter(F.col("new_york_budget") > 38)
    .orderBy(F.col("hr_count").desc())
)
