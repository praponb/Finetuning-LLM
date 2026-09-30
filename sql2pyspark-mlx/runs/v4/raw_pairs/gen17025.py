from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .select(
        F.col("d.budget"),
        F.col("e.full_name"),
        F.col("e.email"),
        F.col("e.is_remote"),
    )
    .orderBy("budget", "full_name")
)
