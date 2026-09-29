from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"))
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .select(F.col("d.id"), F.col("d.location"), F.col("ds.level_values"))
    .orderBy("level_values")
)
