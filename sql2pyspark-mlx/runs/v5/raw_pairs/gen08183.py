from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("dept_name").isin(["Engineering", "Finance"]), "priority").otherwise("other").alias("dept_name_group"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "Berlin", 1).otherwise(0)).alias("berlin_count"),
        F.count("*").alias("total_count"),
    )
    .orderBy(F.col("dept_name_group").desc())
)
