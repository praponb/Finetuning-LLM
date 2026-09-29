from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("full_name").like("bob%"))
    .groupBy("dept_id")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.countDistinct("salary").alias("unique_managers"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.avg_salary") >= 71)
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("d.budget"),
        F.col("t.avg_salary"),
        F.col("t.unique_managers"),
    )
    .orderBy(F.col("location").desc())
)
