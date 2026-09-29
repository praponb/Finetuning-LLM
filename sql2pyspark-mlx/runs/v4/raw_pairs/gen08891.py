from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("job_title") == "Intern") & F.col("dept_id").isin([12, 17, 28]))
    .select("id", "level", "job_title", F.upper("email").alias("email_upper"))
    .orderBy("level")
)
