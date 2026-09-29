from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("departments.id") == F.col("employees.dept_id"), "inner")
    .filter(F.col("departments.dept_name").isin(["Finance", "HR", "Sales"]))
    .groupBy(
        F.when(F.col("departments.budget") >= 4405000, "high").when(F.col("departments.budget") >= 1607000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.max(F.col("employees.hire_date")).alias("last_hire_date"))
    .orderBy(F.col("last_hire_date").desc(), F.col("budget_bucket").desc())
)
