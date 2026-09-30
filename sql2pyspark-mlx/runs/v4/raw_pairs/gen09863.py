from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.max("hire_date").alias("last_hire_date"),
        F.countDistinct("salary").alias("unique_depts"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "inner")
    .select(
        F.col("d.location"),
        F.col("d.budget"),
        F.col("ebd.last_hire_date"),
        F.col("ebd.unique_depts"),
    )
)
