from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("e.dept_id").isin([23, 25, 27, 33]) & F.col("d.location").isNotNull())
    .select(
        F.col("e.id"),
        F.col("e.is_remote"),
        F.col("e.full_name"),
        F.col("d.budget"),
        F.col("d.location"),
    )
)
