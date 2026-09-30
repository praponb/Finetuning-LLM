from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.sum("salary").alias("total_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.total_salary") >= 365000)
    .select(F.col("d.location"), F.col("ds.level_list"), F.col("ds.total_salary"))
    .orderBy(F.col("total_salary").desc(), F.col("location").desc())
)
