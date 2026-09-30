from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("units").asc_nulls_first())
w2 = Window.partitionBy("store_id").orderBy(F.col("revenue").desc_nulls_last())
w_part = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_store_sales
    .withColumn("units_cume_dist", F.cume_dist().over(w))
    .withColumn("revenue_percent_rank", F.percent_rank().over(w2))
    .withColumn("last_known_revenue", F.last("revenue", ignorenulls=True).over(w_part))
)

result = (
    enriched
    .groupBy("product_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.stddev_samp("units").alias("units_stddev"),
        F.round(F.avg("revenue_percent_rank"), 2).alias("avg_revenue_percent_rank"),
        F.percentile_approx("revenue_percent_rank", [0.25, 0.5, 0.75]).alias("revenue_percent_rank_quartiles"),
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
    )
)
