from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.count(F.when(F.col("level") == "L5", 1)).alias("l5_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("t.job_title_list"),
        F.col("t.l5_count"),
    )
    .orderBy(F.col("dept_name").desc(), F.col("l5_count").desc())
)
