from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_year")

result = (
    df_products
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(F.sum("cost").alias("total_cost"))
    .withColumn("cumulative_cost", F.sum("total_cost").over(w))
)
