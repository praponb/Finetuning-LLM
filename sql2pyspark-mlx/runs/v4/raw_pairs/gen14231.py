from pyspark.sql import functions as F

src = (
    df_employees
    .filter((F.length("email") > 14) & F.col("salary").between(82000, 100000))
    .withColumn("days_since_hire_date", F.datediff(F.current_date(), "hire_date"))
)

agg = (
    src
    .groupBy("job_title")
    .agg(
        F.round(F.stddev("days_since_hire_date"), 2).alias("stddev_days_since_hire_date"),
        F.count("days_since_hire_date").alias("days_since_hire_date_count"),
    )
)

result = (
    agg
    .filter(F.col("days_since_hire_date_count") >= 6)
    .select("job_title", "stddev_days_since_hire_date")
    .orderBy(F.col("job_title").desc())
)
