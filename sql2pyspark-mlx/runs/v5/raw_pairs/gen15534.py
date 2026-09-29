from pyspark.sql import functions as F

src = df_employees.filter(F.col("job_title").isin(["Director", "Intern", "Analyst"]))

summary = (
    src
    .groupBy("is_remote", F.month("hire_date").alias("hire_month"))
    .agg(F.max("hire_date").alias("latest_hire_date"))
)

result = summary.select("is_remote", "latest_hire_date")
