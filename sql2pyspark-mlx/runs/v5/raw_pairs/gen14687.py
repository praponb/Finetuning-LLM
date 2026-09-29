from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("e.full_name").isNull())
    .select(
        F.col("e.id"),
        F.col("e.dept_id"),
        F.col("e.is_remote"),
        F.col("e.salary"),
        F.col("d.dept_name"),
        F.col("d.location"),
        F.lower(F.col("d.dept_name")).alias("dept_name_lower"),
    )
)
