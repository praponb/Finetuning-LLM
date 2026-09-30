from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("credit_limit")
w2 = Window.partitionBy("country")

result = (
    df_customers
    .withColumn("credit_limit_dense_rank", F.dense_rank().over(w))
    .withColumn("age_share", F.round((F.col("age") / F.sum("age").over(w2)), 4))
)
