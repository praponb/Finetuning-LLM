from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units_p90").desc())

result = (
    df_store_sales
    .groupBy("store_id", "product_id")
    .agg(
        F.bit_or("units").alias("units_bit_or"),
        F.percentile_approx("units", 0.9).alias("units_p90"),
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
    )
    .withColumn("units_p90_percent_rank", F.percent_rank().over(w))
)
