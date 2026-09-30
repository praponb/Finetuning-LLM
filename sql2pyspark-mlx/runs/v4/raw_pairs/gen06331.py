from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_salary").desc())

result = (
    df_employees
    .groupBy("is_remote")
    .agg(F.sum("salary").alias("total_salary"))
    .withColumn("rnk", F.dense_rank().over(w))
)
