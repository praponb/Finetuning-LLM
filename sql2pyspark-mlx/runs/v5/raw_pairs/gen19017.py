from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("level").isin(["L4", "L1", "L5"]))
    .withColumn("level_job_title", F.concat("level", F.lit(" "), "job_title"))
    .withColumn("salary_whole", F.floor("salary"))
)
