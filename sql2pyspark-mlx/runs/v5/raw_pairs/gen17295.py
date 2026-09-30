from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "inner")
    .filter(F.col("employees.salary").between(92000, 136000))
    .groupBy(
        F.when(F.col("departments.budget") >= 3581000, "high").when(F.col("departments.budget") >= 1593000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.countDistinct(F.col("employees.salary")).alias("unique_depts"))
    .orderBy("budget_bucket")
)
