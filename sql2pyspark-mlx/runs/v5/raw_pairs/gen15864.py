from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location", "dept_name")
    .agg(
        F.min("budget").alias("min_budget"),
        F.max("budget").alias("max_budget"),
        F.countDistinct("budget").alias("unique_dept_names"),
    )
    .orderBy("min_budget")
)
