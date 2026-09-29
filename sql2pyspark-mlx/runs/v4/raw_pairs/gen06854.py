from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("e.level") == "L3")
    .groupBy(F.col("d.dept_name"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.round(((100 * F.sum(F.when(F.col("e.job_title") == "Director", 1).otherwise(0))) / F.count("*")), 1).alias("pct_director"),
    )
    .orderBy(F.col("pct_director").desc(), "dept_name")
)
