from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .select("job_title")
    .distinct()
    .orderBy("job_title")
    .limit(20)
)
