from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("job_title")

result = (
    df_employees
    .groupBy("job_title")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_salary", F.sum("total_salary").over(w))
)
