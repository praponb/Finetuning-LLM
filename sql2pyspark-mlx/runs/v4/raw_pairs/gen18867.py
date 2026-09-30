from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary")

result = (
    df_employees
    .filter(F.col("salary") <= 78000)
    .select("id", "job_title", "salary", F.dense_rank().over(w).alias("dense_rnk"))
)
