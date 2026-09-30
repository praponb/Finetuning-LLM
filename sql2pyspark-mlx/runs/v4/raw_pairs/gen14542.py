from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter(~F.col("is_remote") & (F.col("salary") > 136000))
    .withColumn("job_title_prefix", F.substring("job_title", 1, 2))
)

agg = (
    filtered_employees
    .groupBy("job_title")
    .agg(F.min("salary").alias("min_salary"))
)

result = (
    agg
    .filter(F.col("min_salary") < 123000)
    .orderBy(F.col("min_salary").desc())
)
