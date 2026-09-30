from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("units")

latest = (
    df_store_sales
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "store_id", "units", "revenue")
)

result = (
    latest
    .groupBy("store_id")
    .agg(
        F.percentile_approx("revenue", 0.5).alias("revenue_p50"),
        F.min("units").alias("min_units"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
    )
    .orderBy(F.col("min_units").asc_nulls_first())
)
