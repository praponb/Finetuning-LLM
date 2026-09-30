from pyspark.sql import functions as F

result = (
    df_employees
    .withColumn("full_name_length", F.length("full_name"))
    .withColumn("job_title_lower", F.lower("job_title"))
)
