from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("quarter_start").orderBy(F.col("revenue_p50").desc())

result = (
    df_store_sales
    .groupBy(F.date_trunc("QUARTER", "sale_date").alias("quarter_start"), "product_id")
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.count("*").alias("event_count"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.percentile_approx("revenue", 0.5).alias("revenue_p50"),
        F.min("units").alias("min_units"),
    )
    .filter(F.col("event_count") > 1)
    .withColumn("revenue_p50_dense_rank", F.dense_rank().over(w))
)
