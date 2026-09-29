from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("cost").desc_nulls_last(), "id")

latest = (
    df_products
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .groupBy("category")
    .agg(
        F.stddev_samp("stock_qty").alias("stock_qty_stddev"),
        F.sum("cost").alias("total_cost"),
        F.countDistinct("brand").alias("distinct_brands"),
    )
)
