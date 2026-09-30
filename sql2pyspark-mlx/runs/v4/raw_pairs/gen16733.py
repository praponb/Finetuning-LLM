from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("manager_id").isNull())
    .groupBy("dept_id")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.avg("salary").alias("avg_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("D"), F.col("d.id") == F.col("D.dept_id"), "inner")
    .filter(F.col("D.avg_salary") >= 96000)
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("D.level_list"),
        F.col("D.avg_salary"),
    )
)
