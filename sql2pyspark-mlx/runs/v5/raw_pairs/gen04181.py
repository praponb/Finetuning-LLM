from pyspark.sql import functions as F

filtered_departments = df_departments.filter(F.col("location") != "New York")

summary = (
    filtered_departments
    .groupBy("dept_name")
    .agg(
        F.countDistinct("budget").alias("unique_locations"),
        F.sum("budget").alias("total_budget"),
    )
)

result = (
    summary
    .filter(F.col("unique_locations") >= 6)
    .select("dept_name", "unique_locations", "total_budget")
    .orderBy(F.col("dept_name").desc(), F.col("total_budget").desc())
)
