from pyspark.sql import functions as F

result = (
    df_employees
    .select("level", "job_title")
    .distinct()
    .orderBy(F.col("job_title").desc())
)
