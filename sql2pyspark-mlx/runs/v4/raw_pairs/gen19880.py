from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("email").like("%@startup.io"))
    .groupBy("dept_id")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("E"), F.col("d.id") == F.col("E.dept_id"), "left")
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("d.budget"),
        F.col("E.total_salary"),
        F.col("E.avg_salary"),
    )
    .orderBy(F.col("id").desc())
)
