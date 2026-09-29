from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(F.max("hire_date").alias("latest_hire_date"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("t.latest_hire_date"),
    )
)
