from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("country")

result = (
    df_suppliers
    .filter(F.col("rating").between(3.0, 3.4))
    .groupBy("country")
    .agg(F.sum("rating").alias("total_rating"))
    .withColumn("cumulative_rating", F.sum("total_rating").over(w))
)
