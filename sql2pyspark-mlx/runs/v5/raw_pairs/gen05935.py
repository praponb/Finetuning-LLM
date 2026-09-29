from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("salary") <= 106000)
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("level") == "L5", F.col("salary")).otherwise(0)).alias("l5_salary"),
        F.sum("salary").alias("total_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "inner")
    .select(
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("ebd.l5_salary"),
        F.col("ebd.total_salary"),
    )
)
