from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .select(
        F.col("e.id"),
        F.col("e.level"),
        F.col("e.hire_date"),
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.coalesce(F.col("d.dept_name"), F.lit("unassigned")).alias("dept_name_filled"),
    )
)
