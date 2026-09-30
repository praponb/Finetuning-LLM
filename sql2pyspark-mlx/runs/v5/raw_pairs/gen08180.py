from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("salary") >= 118000)
    .groupBy("dept_id")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"))
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .select(F.col("d.id"), F.col("d.location"), F.col("t.level_values"))
    .orderBy(F.col("location").desc())
)
