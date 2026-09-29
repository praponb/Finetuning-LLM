from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "left")
    .groupBy(F.col("d.dept_name"))
    .agg(
        F.countDistinct(F.col("e.salary")).alias("unique_levels"),
        F.max(F.col("e.hire_date")).alias("last_hire_date"),
    )
    .filter(F.col("unique_levels") > 1)
    .orderBy("unique_levels")
)
