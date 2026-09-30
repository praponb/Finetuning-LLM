from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")

latest = (
    df_order_items
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .groupBy("order_id")
    .agg(
        F.percentile_approx("unit_price", 0.9).alias("unit_price_p90"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("cnt"),
        F.count("*").alias("num_rows_2"),
    )
)
