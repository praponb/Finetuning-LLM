from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(F.sort_array(F.collect_set("job_title")).alias("job_title_list"))
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("D"), F.col("d.id") == F.col("D.dept_id"), "inner")
    .select(F.col("d.dept_name"), F.col("D.job_title_list"))
    .orderBy("dept_name")
)
