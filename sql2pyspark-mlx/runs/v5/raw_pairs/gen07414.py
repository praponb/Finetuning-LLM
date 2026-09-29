from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L2", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l2"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.pct_l2") > 47)
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("t.pct_l2"),
        F.col("t.job_title_list"),
    )
)
