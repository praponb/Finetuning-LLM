from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("level") == "L4", 1).otherwise(0)).alias("l4_count"),
        F.countDistinct("salary").alias("unique_job_titles"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "left")
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("dt.l4_count"),
        F.col("dt.unique_job_titles"),
    )
    .orderBy(F.col("id").desc())
)
