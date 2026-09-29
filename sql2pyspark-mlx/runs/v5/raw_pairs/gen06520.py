from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(F.countDistinct("salary").alias("unique_managers"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("ebd.unique_managers"),
    )
    .orderBy("location")
)
