from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_salary").desc())

result = (
    df_employees
    .filter(F.lower("email").like("%ack%"))
    .groupBy("job_title")
    .agg(F.sum("salary").alias("total_salary"))
    .withColumn("salary_rank", F.rank().over(w))
    .orderBy("total_salary", F.col("salary_rank").desc())
)
