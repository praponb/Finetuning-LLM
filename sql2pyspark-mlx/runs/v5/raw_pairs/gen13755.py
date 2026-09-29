from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("stock_qty").desc())

latest = (
    df_products
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .rollup("category")
    .agg(
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.corr("stock_qty", "price").alias("corr_stock_qty_price"),
        F.max("cost").alias("max_cost"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
    )
    .orderBy("category")
)
