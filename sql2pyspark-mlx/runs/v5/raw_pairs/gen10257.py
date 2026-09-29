from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2023-10-24", "2025-09-01"))
    .withColumn("level_lower", F.lower("level"))
    .withColumn("job_title_length", F.length("job_title"))
    .orderBy("job_title_length", F.col("level_lower").desc())
)
