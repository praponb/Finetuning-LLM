from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_revenue").desc())

result = (
    df_store_sales
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(F.sum("revenue").alias("total_revenue"))
    .withColumn("revenue_rank", F.rank().over(w))
    .orderBy("sale_month")
)
