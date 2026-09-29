from pyspark.sql import functions as F

base = (
    df_departments
    .filter((F.col("dept_name") != "Engineering") & (F.col("budget") > 1424000))
)

location_summary = (
    base
    .groupBy("location")
    .agg(
        F.countDistinct("budget").alias("unique_dept_names"),
        F.min("budget").alias("min_budget"),
    )
)

result = location_summary.select("location", "min_budget")
