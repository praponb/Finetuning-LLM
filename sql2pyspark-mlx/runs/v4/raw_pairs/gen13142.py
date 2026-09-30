from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter((F.col("d.budget") > 1251000) | (F.upper(F.trim(F.col("e.full_name"))) == "NOAH PARK"))
    .select(
        F.col("e.salary"),
        F.col("e.email"),
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("d.budget"),
    )
    .orderBy(F.col("budget").desc())
)
