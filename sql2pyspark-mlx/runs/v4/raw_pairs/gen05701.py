from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("salary").desc())

result = (
    df_employees
    .select("id", "job_title", "salary", F.dense_rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 1)
    .select("id", "job_title", "salary")
)
