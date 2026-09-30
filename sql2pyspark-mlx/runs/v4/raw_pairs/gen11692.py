from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1592000, 3377000))
    .agg(
        F.countDistinct("budget").alias("unique_dept_names"),
        F.min("budget").alias("min_budget"),
    )
)
