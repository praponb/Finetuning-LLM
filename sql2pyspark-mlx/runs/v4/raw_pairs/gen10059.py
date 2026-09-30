from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "right")
    .filter(F.col("d.budget") <= 3566000)
    .groupBy(F.col("d.dept_name"))
    .agg(
        F.min(F.col("e.salary")).alias("min_salary"),
        F.sort_array(F.collect_set(F.col("e.level"))).alias("level_list"),
    )
)
