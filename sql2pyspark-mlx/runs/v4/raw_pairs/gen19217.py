from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("e.hire_date").between("2025-02-27", "2025-07-01"))
    .select(
        F.col("e.id"),
        F.col("e.dept_id"),
        F.col("e.hire_date"),
        F.col("d.dept_name"),
        F.col("d.location"),
    )
)
