from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("job_title").isin(["Designer", "Engineer"]) & F.col("job_title").isNotNull() & F.col("job_title").isNotNull())
    .groupBy(F.month("hire_date").alias("hire_month"), "job_title")
    .agg(F.max("salary").alias("max_salary"))
)
