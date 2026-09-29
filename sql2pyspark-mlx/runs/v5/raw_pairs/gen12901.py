from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("level") == "L4", F.col("salary")).otherwise(0)).alias("l4_salary"),
        F.avg("salary").alias("avg_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .select(
        F.col("d.budget"),
        F.col("d.location"),
        F.col("t.l4_salary"),
        F.col("t.avg_salary"),
    )
    .orderBy(F.col("avg_salary").desc())
)
