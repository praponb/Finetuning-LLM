from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("budget") <= 2122000).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_anti")
    .filter(F.month(F.col("e.hire_date")).isin([1, 3, 4]))
    .select(
        F.col("e.id"),
        F.col("e.level"),
        F.col("e.hire_date"),
        F.col("e.manager_id"),
    )
    .orderBy(F.col("hire_date").desc())
)
