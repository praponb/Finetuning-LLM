from pyspark.sql import functions as F

result = (
    df_employees
    .withColumn("hire_date_period", F.date_trunc("MONTH", "hire_date"))
    .withColumn("level_job_title", F.concat("level", F.lit("/"), "job_title"))
    .orderBy(F.col("id").desc())
)
