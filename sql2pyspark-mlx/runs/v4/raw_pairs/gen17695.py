from pyspark.sql import functions as F

result = (
    df_departments
    .filter(((F.col("budget") < 1364000) & (F.col("budget") >= 3292000)) | F.col("budget").between(1545000, 2050000))
    .groupBy("dept_name")
    .agg(
        F.countDistinct("budget").alias("unique_locations"),
        F.max("budget").alias("max_budget"),
    )
)
