from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("product_id").orderBy(F.col("distinct_store_ids").desc())

metrics = (
    df_store_sales
    .withColumn("units_share", F.round((F.col("units") / F.sum("units").over(w)), 4))
    .withColumn("units_share_2", F.round((F.col("units") / F.sum("units").over(w2)), 4))
)

result = (
    metrics
    .groupBy("product_id", F.date_trunc("YEAR", "sale_date").alias("year_start"))
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.stddev_samp("units").alias("units_stddev"),
    )
    .withColumn("distinct_store_ids_rank", F.rank().over(w3))
)
