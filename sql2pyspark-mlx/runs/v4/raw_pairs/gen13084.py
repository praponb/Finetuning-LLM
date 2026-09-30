from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("e.dept_id").isin([6, 8, 17]))
    .select(
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("e.manager_id"),
        F.col("e.hire_date"),
    )
    .orderBy("dept_name")
)
