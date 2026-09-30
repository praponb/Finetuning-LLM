from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title", "dept_id")
    .agg(
        F.max("hire_date").alias("last_hire_date"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Designer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_designer"),
    )
    .filter(F.col("pct_designer") > 44)
    .orderBy(F.col("dept_id").desc())
)
