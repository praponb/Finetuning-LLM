from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("credit_limit")

result = (
    df_customers
    .filter(F.col("is_active"))
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") == 1)
)
