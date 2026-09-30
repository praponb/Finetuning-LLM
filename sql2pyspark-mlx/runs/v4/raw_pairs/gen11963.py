from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary")

result = (
    df_employees
    .filter(F.upper("email").like("%RAC%"))
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 5)
    .select("id", "job_title", "salary")
)
