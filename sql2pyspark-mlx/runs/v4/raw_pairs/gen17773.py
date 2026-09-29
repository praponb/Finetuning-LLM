from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([16, 19, 33]))
    .groupBy("is_remote", "job_title")
    .agg(
        F.sum(F.when(F.col("job_title") == "Analyst", F.col("salary")).otherwise(0)).alias("analyst_salary"),
    )
    .filter(F.col("analyst_salary") > 48)
    .select("is_remote", "job_title", "analyst_salary")
    .orderBy(F.col("analyst_salary").desc())
)
