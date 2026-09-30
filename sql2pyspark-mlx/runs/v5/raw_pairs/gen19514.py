from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc())

deduped = (
    df_store_sales
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "store_id", "product_id", "sale_date", "revenue")
)

result = (
    deduped
    .groupBy(F.date_trunc("week", "sale_date").alias("week_start"))
    .agg(
        F.stddev_samp("revenue").alias("revenue_stddev"),
        F.stddev_samp("revenue").alias("revenue_stddev_2"),
        F.stddev_samp("revenue").alias("revenue_stddev_3"),
    )
    .filter(F.col("revenue_stddev_2") > 0)
    .orderBy(F.col("revenue_stddev_2").desc())
)
