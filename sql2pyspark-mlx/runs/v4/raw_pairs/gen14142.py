from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("salary") > 69000)
    .groupBy("dept_id")
    .agg(
        F.min("hire_date").alias("min_hire_date"),
        F.sum(F.when(F.col("job_title") == "Engineer", 1).otherwise(0)).alias("engineer_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("ds.min_hire_date"),
        F.col("ds.engineer_count"),
    )
    .orderBy("min_hire_date", F.col("engineer_count").desc())
)
