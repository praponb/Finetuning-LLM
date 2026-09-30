from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("job_title") == "Designer")
    .groupBy("dept_id")
    .agg(
        F.count("*").alias("num_employees"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("D"), F.col("d.id") == F.col("D.dept_id"), "left")
    .select(F.col("d.budget"), F.col("D.num_employees"), F.col("D.stddev_salary"))
    .orderBy("stddev_salary")
)
