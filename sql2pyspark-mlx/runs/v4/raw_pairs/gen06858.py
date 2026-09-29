from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("dept_id").isin([4, 8, 20, 28]))
    .groupBy("dept_id")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.stddev_salary") > 79)
    .select(
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("ds.avg_salary"),
        F.col("ds.stddev_salary"),
    )
)
