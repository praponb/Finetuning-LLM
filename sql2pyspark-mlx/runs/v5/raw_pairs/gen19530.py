from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.round(((100 * F.sum(F.when(F.col("level") == "L3", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l3"),
    )
)

result = (
    df_departments.alias("departments")
    .join(employees_by_department.alias("t"), F.col("departments.id") == F.col("t.dept_id"), "left")
    .select(F.col("departments.dept_name"), F.col("t.total_salary"), F.col("t.pct_l3"))
    .orderBy(F.col("total_salary").desc(), F.col("dept_name").desc())
)
