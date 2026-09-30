from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .select("job_title")
    .subtract(df_employees.filter(~F.col("is_remote")).select("job_title"))
    .orderBy(F.col("job_title").desc())
)
