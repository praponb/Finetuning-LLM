from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "left_anti")
    .filter(F.col("departments.location").isin(["Chiang Mai", "Tokyo"]))
    .select(
        F.col("departments.id"),
        F.col("departments.budget"),
        F.col("departments.dept_name"),
    )
    .orderBy("dept_name", "id")
    .limit(5)
)
