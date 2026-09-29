from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Analyst", "Director", "Engineer"]))
    .select("job_title")
    .distinct()
)
