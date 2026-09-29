from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("D"), F.col("d.id") == F.col("D.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("D.level_values"),
        F.col("D.job_title_list"),
    )
    .orderBy(F.col("id").desc())
    .limit(1)
)
