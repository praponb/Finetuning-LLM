from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.max("hire_date").alias("latest_hire_date"),
        F.sum("salary").alias("total_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("E"), F.col("d.id") == F.col("E.dept_id"), "inner")
    .filter(F.col("E.total_salary") >= 742000)
    .select(
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("E.latest_hire_date"),
        F.col("E.total_salary"),
    )
    .orderBy("total_salary")
)
