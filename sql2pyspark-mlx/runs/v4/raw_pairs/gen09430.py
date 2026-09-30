from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.max("hire_date").alias("latest_hire_date"),
        F.avg("salary").alias("avg_salary"),
    )
    .filter(F.col("avg_salary") >= 120000)
    .orderBy("latest_hire_date", "job_title")
    .limit(20)
)
