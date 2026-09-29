from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.max("salary").alias("max_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .filter(F.col("ds.max_salary") > 113000)
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("ds.stddev_salary"),
        F.col("ds.max_salary"),
    )
    .orderBy("stddev_salary")
)
