from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("is_remote"))
    .groupBy("dept_id")
    .agg(F.round(F.stddev("salary"), 2).alias("stddev_salary"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.stddev_salary") > 62)
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("d.budget"),
        F.col("t.stddev_salary"),
    )
    .orderBy("location", "id")
    .limit(3)
)
