from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.countDistinct("budget").alias("unique_dept_names"),
        F.sum(F.when(F.col("location") == "Singapore", F.col("budget")).otherwise(0)).alias("singapore_budget"),
    )
    .filter(F.col("singapore_budget") >= 48)
    .select("dept_name", "unique_dept_names", "singapore_budget")
    .orderBy(F.col("dept_name").desc())
)
