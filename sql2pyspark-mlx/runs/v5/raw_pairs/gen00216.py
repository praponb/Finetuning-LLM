from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("country")

result = (
    df_customers
    .filter(F.col("email").isNull())
    .groupBy("country")
    .agg(F.sum("age").alias("total_age"))
    .withColumn("cumulative_age", F.sum("total_age").over(w))
    .orderBy(F.col("country").desc())
)
