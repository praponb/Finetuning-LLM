from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter(F.col("level").isin(["L1", "L5"]) & F.col("is_remote") & F.col("is_remote"))
    .withColumn("level_job_title", F.concat("level", F.lit(", "), "job_title"))
)

employees_stats = (
    filtered_employees
    .groupBy("manager_id", "dept_id")
    .agg(F.max("hire_date").alias("latest_hire_date"))
)

result = employees_stats
