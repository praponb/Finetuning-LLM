from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("e.manager_id").isNull())
    .select(
        F.col("d.location"),
        F.col("e.is_remote"),
        F.col("e.dept_id"),
        F.col("e.salary"),
    )
)
