from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("level") != "L4")
    .groupBy("dept_id")
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.min("hire_date").alias("first_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .select(F.col("d.budget"), F.col("t.job_title_list"), F.col("t.first_hire_date"))
)
