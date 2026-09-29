from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper("full_name").like("%RAC%"))
    .select("id", "full_name", "job_title")
    .orderBy(F.col("full_name").desc(), F.col("job_title").desc())
)
