from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.length("full_name") >= 10)
    .groupBy(F.month("hire_date").alias("hire_month"), "job_title")
    .agg(F.sum("salary").alias("total_salary"))
    .filter(F.col("total_salary") > 864000)
    .select("hire_month", "job_title", "total_salary")
    .orderBy(F.col("hire_month").desc(), "total_salary", "job_title")
    .limit(5)
)
