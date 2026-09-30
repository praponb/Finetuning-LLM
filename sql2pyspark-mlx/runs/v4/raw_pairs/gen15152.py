from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Tokyo")
    .groupBy("dept_name", "location")
    .agg(
        F.sum(F.when(F.col("dept_name") == "Engineering", 1).otherwise(0)).alias("engineering_count"),
        F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_count"),
    )
    .orderBy("location")
)
