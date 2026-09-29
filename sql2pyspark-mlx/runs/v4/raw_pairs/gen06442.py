from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .filter(F.length("email") < 5)
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "level", "salary", "job_title")
    .orderBy(F.col("id").desc(), "job_title")
)
