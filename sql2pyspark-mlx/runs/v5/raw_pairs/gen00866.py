from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(
        F.min("salary").alias("min_salary"),
        F.max("salary").alias("max_salary"),
        F.count("*").alias("cnt"),
    )
    .orderBy("job_title")
)
