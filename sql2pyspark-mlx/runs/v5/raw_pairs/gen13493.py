from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(F.countDistinct("salary").alias("unique_managers"))
)

result = (
    df_departments.alias("departments")
    .join(employees_by_department.alias("EmployeesByDepartment"), F.col("departments.id") == F.col("EmployeesByDepartment.dept_id"), "left")
    .filter(F.col("EmployeesByDepartment.unique_managers") >= 6)
    .select(
        F.col("departments.id"),
        F.col("departments.dept_name"),
        F.col("departments.budget"),
        F.col("EmployeesByDepartment.unique_managers"),
    )
)
