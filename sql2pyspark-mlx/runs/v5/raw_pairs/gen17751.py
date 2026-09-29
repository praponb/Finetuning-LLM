from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Manager", 1).otherwise(0))) / F.count("*")), 1).alias("pct_manager"),
    )
)

result = (
    df_departments.alias("departments")
    .join(employees_by_department.alias("EmployeesByDepartment"), F.col("departments.id") == F.col("EmployeesByDepartment.dept_id"), "left")
    .filter(F.col("EmployeesByDepartment.pct_manager") >= 73)
    .select(
        F.col("departments.location"),
        F.col("departments.budget"),
        F.col("EmployeesByDepartment.avg_salary"),
        F.col("EmployeesByDepartment.pct_manager"),
    )
    .orderBy(F.col("avg_salary").desc())
)
