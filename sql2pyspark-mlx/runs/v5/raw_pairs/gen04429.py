from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "left")
    .filter(F.col("departments.location") == "Bangkok")
    .groupBy(
        F.when(F.col("departments.budget") >= 3901000, "high").when(F.col("departments.budget") >= 1908000, "mid").otherwise("low").alias("budget_bucket"),
        F.col("departments.dept_name"),
    )
    .agg(
        F.sum(F.when(F.col("employees.level") == "L2", 1).otherwise(0)).alias("l2_count"),
        F.max(F.col("employees.salary")).alias("max_salary"),
    )
    .filter(F.col("max_salary") > 58000)
)
