from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") < 130000)
    .select("level", "job_title")
    .distinct()
)
