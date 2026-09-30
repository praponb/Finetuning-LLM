from pyspark.sql import functions as F

cleaned = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .withColumn("salary_int", F.col("salary").cast("int"))
)

result = (
    cleaned
    .groupBy(
        F.date_trunc("month", "hire_date").alias("activity_month"),
        F.date_trunc("week", "hire_date").alias("week_start"),
    )
    .agg(
        F.sum(F.when(F.col("job_title") == "Designer", F.col("salary_int"))).alias("designer_total_salary_int"),
        F.sum(F.when(F.col("job_title") == "Analyst", F.col("salary"))).alias("analyst_total_salary"),
        F.count_if(F.col("level") == "L2").alias("l2_events"),
        F.countDistinct("dept_id").alias("distinct_dept_ids"),
        F.count_if(F.col("job_title") == "Manager").alias("manager_events"),
        F.percentile_approx("salary", 0.9).alias("salary_p90"),
    )
    .filter(F.col("analyst_total_salary") < 0)
)
