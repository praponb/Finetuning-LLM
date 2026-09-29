from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("d.dept_name") != "Engineering")
    .groupBy(F.col("d.id"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.max(F.col("e.salary")).alias("max_salary"),
        F.count(F.when(F.col("e.level") == "L5", 1)).alias("l5_count"),
    )
)
