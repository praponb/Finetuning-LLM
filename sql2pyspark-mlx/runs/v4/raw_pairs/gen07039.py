from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("job_title") != "Director")
    .groupBy("dept_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.max("hire_date").alias("latest_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("ds.level_values"),
        F.col("ds.latest_hire_date"),
    )
    .orderBy("latest_hire_date", "level_values")
)
