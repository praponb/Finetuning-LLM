from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("is_remote"))
    .groupBy("dept_id")
    .agg(
        F.countDistinct("salary").alias("unique_depts"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("t.unique_depts"),
        F.col("t.level_values"),
    )
    .orderBy("level_values", "location")
)
