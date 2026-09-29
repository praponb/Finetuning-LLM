from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.month(F.col("e.hire_date")).isin([3, 4, 5]))
    .select(
        F.col("d.location"),
        F.col("d.budget"),
        F.col("e.is_remote"),
        F.col("e.full_name"),
    )
)
