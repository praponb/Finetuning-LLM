from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .select(
        F.col("e.id"),
        F.col("e.level"),
        F.col("e.hire_date"),
        F.col("e.salary"),
        F.col("d.budget"),
        F.col("d.location"),
        F.col("d.dept_name"),
    )
    .orderBy(F.col("level").desc())
)
