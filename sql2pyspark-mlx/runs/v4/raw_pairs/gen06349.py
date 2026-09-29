from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id").rowsBetween(-6, -1)
w2 = Window.orderBy(F.col("stock_qty").asc_nulls_last())
w3 = Window.partitionBy("category", "brand")

base = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-10-16").cast("date"))
    .select(
        "*",
        F.substring("category", 1, 2).alias("category_prefix"),
        F.trim("category").alias("clean_category"),
    )
)

result = (
    base
    .withColumn("cost_avg_6_prior", F.avg("cost").over(w))
    .withColumn("stock_qty_percent_rank", F.percent_rank().over(w2))
    .withColumn("corr_cost_stock_qty", F.corr("cost", "stock_qty").over(w3))
)
