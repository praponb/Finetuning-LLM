from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("activity_month"))
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.percentile_approx("salary", 0.5).alias("salary_p50"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
    .filter(F.col("salary_p50") < 1000000)
    .orderBy("avg_salary")
)
