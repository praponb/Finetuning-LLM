from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date")

result = (
    df_employees
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") == 1)
    .select("id", "dept_id", "hire_date", "job_title")
    .orderBy("job_title")
)
