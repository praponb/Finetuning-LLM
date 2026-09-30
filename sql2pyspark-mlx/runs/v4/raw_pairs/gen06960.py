from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("email").like("%@gmail.com"))
    .withColumn("hire_date_month_end", F.last_day("hire_date"))
    .withColumn("hire_date_minus_23d", F.date_sub("hire_date", 23))
)

result = (
    base
    .groupBy(
        F.when(F.col("salary") >= 158000, "HIGH").otherwise("LOW").alias("salary_tier"),
    )
    .agg(
        F.count_if(F.col("job_title") == "Intern").alias("intern_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("level").alias("distinct_levels"),
        F.sum(F.when(F.col("job_title") == "Designer", F.col("salary")).otherwise(0)).alias("designer_salary"),
    )
)
