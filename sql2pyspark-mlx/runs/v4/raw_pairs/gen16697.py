from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") > 80000) & F.col("is_remote"))
    .withColumn("level_title", F.initcap(F.trim("level")))
    .withColumn("level_job_title", F.concat("level", F.lit(" "), "job_title"))
)
