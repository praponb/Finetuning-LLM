from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.filter(~F.col("location").isin(["Berlin", "Singapore", "London"])).alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "left_anti")
    .select(
        F.col("employees.id"),
        F.col("employees.hire_date"),
        F.col("employees.full_name"),
    )
    .orderBy(F.col("full_name").desc(), "id")
    .limit(50)
)
