from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").isNull())
    .agg(
        F.sum(F.when(F.col("location") == "London", 1).otherwise(0)).alias("london_count"),
        F.countDistinct("budget").alias("unique_dept_names"),
    )
)
