from pyspark.sql import functions as F

result = (
    df_employees
    .select("job_title")
    .distinct()
    .orderBy(F.col("job_title").desc())
    .limit(20)
)
