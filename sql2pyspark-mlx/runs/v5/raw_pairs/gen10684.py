from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("employees.dept_id") == F.col("departments.id"), "left")
    .select(
        F.col("departments.dept_name"),
        F.col("departments.budget"),
        F.col("departments.location"),
        F.col("employees.email"),
    )
    .orderBy("email", "budget")
)
