from pyspark.sql import functions as F

result = (
    df_employees
    .withColumn("job_title_length", F.length("job_title"))
    .withColumn("job_title_full_name_key", F.concat_ws("/", "job_title", "full_name"))
)
