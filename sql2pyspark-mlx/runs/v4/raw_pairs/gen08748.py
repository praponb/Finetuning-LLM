from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("manager_id").isNull())
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("job_title") == "Director", 1).otherwise(0)).alias("director_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("t.director_count"),
        F.col("t.level_values"),
    )
)
