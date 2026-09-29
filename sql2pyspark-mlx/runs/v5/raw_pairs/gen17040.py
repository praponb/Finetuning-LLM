from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .select(
        F.col("d.location"),
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("e.email"),
        F.col("e.manager_id"),
    )
)
