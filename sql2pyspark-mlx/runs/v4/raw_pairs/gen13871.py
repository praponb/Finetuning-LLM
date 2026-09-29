from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.count("*").alias("row_count"),
        F.count("budget").alias("budget_count"),
    )
    .filter(F.col("budget_count") > 3)
    .drop("budget_count")
    .orderBy("dept_name")
    .limit(10)
)
