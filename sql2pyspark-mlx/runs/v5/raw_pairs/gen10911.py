from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_orders
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(F.sum("discount").alias("total_discount"))
    .withColumn("pct_of_total", F.round(((F.col("total_discount") * 100) / F.sum("total_discount").over(w)), 2))
)
