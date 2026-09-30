from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter((F.year(F.col("e.hire_date")) == 2025) & ~F.col("e.level").isin(["L3", "L1"]))
    .select(
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("d.location"),
        F.col("e.is_remote"),
        F.col("e.dept_id"),
        F.col("e.salary"),
    )
)
