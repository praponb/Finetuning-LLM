from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "left")
    .filter(F.col("employees.salary") > 142000)
    .groupBy(
        F.when(F.col("departments.budget") >= 3484000, "high").when(F.col("departments.budget") >= 1091000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.col("employees.salary")).alias("total_salary"),
        F.countDistinct(F.col("employees.salary")).alias("unique_managers"),
    )
)
