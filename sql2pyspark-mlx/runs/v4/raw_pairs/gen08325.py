from pyspark.sql import functions as F

result = (
    df_departments
    .rollup("dept_name")
    .agg(
        F.countDistinct("location").alias("distinct_locations"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
)
