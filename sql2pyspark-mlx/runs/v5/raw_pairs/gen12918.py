from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("e.email").like("%@mail.co.th"))
    .select(
        F.col("d.location"),
        F.col("d.budget"),
        F.col("e.manager_id"),
        F.col("e.is_remote"),
        F.col("e.full_name"),
    )
)
