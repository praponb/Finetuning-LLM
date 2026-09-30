from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary")

result = (
    df_employees
    .select("id", "job_title", "salary", "is_remote", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 2)
    .select("id", "job_title", "salary", "is_remote")
    .orderBy(F.col("salary").desc())
)
