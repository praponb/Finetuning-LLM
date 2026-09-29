from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "right")
    .filter(F.col("departments.budget").isNotNull())
    .groupBy(F.col("departments.dept_name"))
    .agg(
        F.sort_array(F.collect_set(F.col("employees.level"))).alias("level_list"),
        F.round(F.avg(F.col("employees.salary")), 2).alias("avg_salary"),
    )
)
