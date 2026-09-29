from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(
        F.max("hire_date").alias("last_hire_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
    .filter(F.col("avg_salary") < 30)
    .orderBy(F.col("job_title_values").desc(), F.col("avg_salary").desc())
)
