from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("stock_qty_bit_or").desc())

result = (
    df_products
    .groupBy("category", "brand")
    .agg(
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
    )
    .withColumn("stock_qty_bit_or_dense_rank", F.dense_rank().over(w))
)
