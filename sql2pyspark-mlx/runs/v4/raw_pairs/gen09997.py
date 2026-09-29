from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-05-04").cast("date"))
    .withColumn("clean_job_title", F.trim("job_title"))
)

result = (
    base
    .groupBy("manager_id")
    .agg(
        F.sum(F.when(F.col("job_title") == "Analyst", F.col("salary")).otherwise(0)).alias("analyst_salary"),
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.count_if(F.col("job_title") == "Analyst").alias("analyst_events"),
        F.size(F.collect_set("job_title")).alias("num_job_titles_2"),
        F.min("salary").alias("min_salary"),
        F.count_if(F.col("job_title") == "Intern").alias("intern_events"),
    )
    .filter(F.col("min_salary") >= 0)
)
