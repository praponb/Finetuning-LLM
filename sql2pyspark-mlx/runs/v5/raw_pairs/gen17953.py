from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_cost").desc())

result = (
    df_products
    .filter(F.col("price") > 720)
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(F.sum("cost").alias("total_cost"))
    .withColumn("cost_rank", F.rank().over(w))
)
