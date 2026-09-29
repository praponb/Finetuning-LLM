from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_rating").desc())

result = (
    df_suppliers
    .filter(F.col("rating") < 4.1)
    .groupBy("city")
    .agg(F.sum("rating").alias("total_rating"))
    .withColumn("rnk", F.dense_rank().over(w))
    .orderBy(F.col("total_rating").desc(), F.col("rnk").desc())
)
