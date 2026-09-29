from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("is_remote"))
    .groupBy("dept_id")
    .agg(
        F.count("*").alias("num_employees"),
        F.max("hire_date").alias("last_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("ds.num_employees"),
        F.col("ds.last_hire_date"),
    )
)
