from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_month")

result = (
    df_store_sales
    .filter(F.col("revenue") < 2300)
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(F.sum("revenue").alias("total_revenue"))
    .withColumn("cumulative_revenue", F.sum("total_revenue").over(w))
    .orderBy("sale_month")
)
