from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(F.sort_array(F.collect_set("job_title")).alias("job_title_list"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .select(F.col("d.id"), F.col("d.location"), F.col("dt.job_title_list"))
)
