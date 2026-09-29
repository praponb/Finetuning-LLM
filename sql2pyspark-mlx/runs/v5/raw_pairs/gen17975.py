from pyspark.sql import functions as F

result = (
    df_employees
    .withColumn("job_title_title", F.initcap(F.trim("job_title")))
    .withColumn("level_prefix", F.substring("level", 1, 2))
)
