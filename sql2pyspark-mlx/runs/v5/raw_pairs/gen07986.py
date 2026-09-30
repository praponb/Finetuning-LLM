from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.year("hire_date") >= 2023)
    .groupBy("dept_id")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "left")
    .select(F.col("d.budget"), F.col("dt.level_values"))
    .orderBy(F.col("budget").desc())
)
