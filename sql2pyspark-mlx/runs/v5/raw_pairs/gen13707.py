from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_products
    .filter(F.year("launch_date") >= 2024)
    .groupBy("category")
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.count("*").alias("cnt"),
    )
    .withColumn("pct_of_total", F.round(((F.col("total_stock_qty") * 100) / F.sum("total_stock_qty").over(w)), 2))
)
