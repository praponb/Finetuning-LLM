from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") > 109000)
    .withColumn("salary_whole", F.ceil("salary"))
    .withColumn("level_length", F.length("level"))
    .orderBy(F.col("id").desc(), "full_name")
)
