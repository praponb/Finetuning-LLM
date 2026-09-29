from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("city").orderBy(F.col("sqft").asc_nulls_first())
w3 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-3, -1)
w_ordered = Window.partitionBy("city", "region").orderBy(F.col("opened_date").desc(), "id")
w5 = Window.partitionBy("region").orderBy(F.col("row_seq").desc(), "id")
w6 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("city").orderBy(F.col("sqft_avg_3_prior").asc_nulls_last())
w8 = Window.partitionBy("city", "region")
w9 = Window.partitionBy("city").orderBy("sqft_share", "id")
w10 = Window.orderBy("sqft_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_stores
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w)), 4))
    .withColumn("sqft_rank", F.rank().over(w2))
    .withColumn("sqft_avg_3_prior", F.avg("sqft").over(w3))
    .withColumn("row_seq", F.row_number().over(w_ordered))
)

enriched = (
    metrics
    .withColumn("row_seq_bucket", F.ntile(5).over(w5))
    .withColumn("cumulative_sqft_share", F.sum("sqft_share").over(w6))
    .withColumn("sqft_avg_3_prior_cume_dist", F.cume_dist().over(w_grp))
)

metrics_stores = (
    enriched
    .withColumn("city_stddev_samp_sqft_avg_3_prior_cume_dist", F.stddev_samp("sqft_avg_3_prior_cume_dist").over(w8))
    .withColumn("next_sqft_avg_3_prior_cume_dist", F.lead("sqft_avg_3_prior_cume_dist", 1).over(w9))
    .withColumn("cumulative_sqft", F.sum("sqft").over(w10))
)

result = (
    metrics_stores
    .select(
        "id",
        "store_name",
        "region",
        "sqft",
        "sqft_share",
        "row_seq",
        "row_seq_bucket",
        "cumulative_sqft_share",
        "sqft_avg_3_prior_cume_dist",
    )
    .orderBy(F.col("sqft_avg_3_prior_cume_dist").asc_nulls_last())
)
