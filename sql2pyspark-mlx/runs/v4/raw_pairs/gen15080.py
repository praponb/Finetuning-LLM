from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary")

result = (
    df_employees
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") <= 5)
    .orderBy("email")
)
