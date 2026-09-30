from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.filter(F.col("location").isin(["Tokyo", "Singapore"])).alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "left_semi")
    .filter(F.col("employees.manager_id").isNull())
    .select(
        F.col("employees.id"),
        F.col("employees.salary"),
        F.col("employees.email"),
        F.col("employees.manager_id"),
    )
    .orderBy(F.col("manager_id").desc())
)
