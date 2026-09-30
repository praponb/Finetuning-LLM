from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.countDistinct("budget").alias("unique_dept_names"),
        F.sum("budget").alias("total_budget"),
    )
    .filter(F.col("unique_dept_names") < 9)
)
