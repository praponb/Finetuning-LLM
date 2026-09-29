from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(F.min("salary").alias("min_salary"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("E"), F.col("d.id") == F.col("E.dept_id"), "inner")
    .filter(F.col("E.min_salary") >= 126000)
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("d.location"),
        F.col("E.min_salary"),
    )
)
