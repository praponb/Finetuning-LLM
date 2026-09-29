from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(F.sort_array(F.collect_set("level")).alias("level_list"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .select(F.col("d.budget"), F.col("dt.level_list"))
    .orderBy(F.col("budget").desc())
)
