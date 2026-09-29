from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("job_title") == "Engineer", 1).otherwise(0)).alias("engineer_count"),
        F.count("*").alias("employee_count"),
    )
)

result = (
    df_departments.alias("departments")
    .join(employees_by_department.alias("t"), F.col("departments.id") == F.col("t.dept_id"), "left")
    .filter(F.col("t.employee_count") >= 7)
    .select(
        F.col("departments.dept_name"),
        F.col("t.engineer_count"),
        F.col("t.employee_count"),
    )
    .orderBy(F.col("engineer_count").desc())
)
