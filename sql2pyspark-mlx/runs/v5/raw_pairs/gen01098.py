from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.dept_name"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.sum(F.col("e.salary")).alias("total_salary"),
        F.countDistinct(F.col("e.salary")).alias("unique_managers"),
    )
    .filter(F.col("employee_count") > 4)
    .orderBy(F.col("total_salary").desc())
)
