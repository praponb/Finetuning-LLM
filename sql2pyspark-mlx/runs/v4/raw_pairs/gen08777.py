from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("year_start").orderBy(F.col("min_quantity").desc())

result = (
    df_inventory
    .groupBy(F.date_trunc("YEAR", "last_restock").alias("year_start"), "store_id")
    .agg(
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.max("quantity").alias("max_quantity"),
        F.min("quantity").alias("min_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity_2"),
    )
    .withColumn("min_quantity_rank", F.rank().over(w))
)
