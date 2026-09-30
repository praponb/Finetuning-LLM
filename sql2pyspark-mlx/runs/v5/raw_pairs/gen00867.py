from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("dept_id").isin([7, 14, 24, 25]))
    .groupBy("dept_id")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.sum("salary").alias("total_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "left")
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("dt.stddev_salary"),
        F.col("dt.total_salary"),
    )
    .orderBy(F.col("dept_name").desc())
)
