from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc(), "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("units").asc_nulls_last())
w4 = Window.orderBy(F.col("revenue").asc_nulls_first())
w5 = Window.partitionBy("product_id", "store_id")

latest = (
    df_store_sales
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    latest
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("units_cume_dist", F.cume_dist().over(w3))
    .withColumn("revenue_rank", F.rank().over(w4))
    .withColumn("revenue_share", F.round((F.col("revenue") / F.sum("revenue").over(w5)), 4))
)

valid = (
    enriched
    .filter(F.col("store_id").isin([3, 4, 5]))
    .select(
        "*",
        (F.col("units_cume_dist") - F.col("row_num")).alias("units_cume_dist_minus_row_num"),
    )
)

result = (
    valid
    .groupBy("store_id")
    .agg(
        F.bit_or("units").alias("units_bit_or"),
        F.percentile_approx("seq_no", 0.9).alias("seq_no_p90"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
    )
    .orderBy(F.col("store_id").desc())
)
