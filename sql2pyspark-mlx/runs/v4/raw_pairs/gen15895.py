from pyspark.sql import functions as F

src = (
    df_employees
    .filter(F.col("dept_id").isin([8, 14, 18, 19]))
    .withColumn("email_domain", F.regexp_extract("email", "@(.+)$", 1))
)

summary = (
    src
    .groupBy("job_title")
    .agg(
        F.sum(F.when(F.col("job_title") == "Director", F.col("salary")).otherwise(0)).alias("director_salary"),
    )
)

result = summary
