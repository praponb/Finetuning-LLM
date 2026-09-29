from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sqft", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w3 = Window.partitionBy("city").orderBy(F.col("sqft").desc())
w_hist = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city").orderBy("opened_date", "id")
w6 = Window.partitionBy("city", "region").orderBy(F.col("sqft_avg_6").asc_nulls_first())
w7 = Window.partitionBy("city", "region").orderBy("sqft_avg_6", "id").rowsBetween(-3, -1)
w8 = Window.partitionBy("city")
w9 = Window.partitionBy("region", "city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy(F.col("sqft_avg_6").desc(), "id")
w11 = Window.partitionBy("city").orderBy(F.col("running_sqft_cume_dist").desc(), "id")
w12 = Window.orderBy("seq_no", "id")
w13 = Window.partitionBy("region", "city")

metrics = (
    df_stores
    .select(
        "*",
        F.avg("sqft").over(w).alias("sqft_avg_6"),
        F.row_number().over(w2).alias("seq_no"),
        F.cume_dist().over(w3).alias("sqft_cume_dist"),
    )
)

scored = (
    metrics
    .withColumn("running_sqft_cume_dist", F.sum("sqft_cume_dist").over(w_hist))
    .withColumn("prev_seq_no", F.lag("seq_no", 1).over(w5))
)

enriched = (
    scored
    .select(
        "*",
        F.dense_rank().over(w6).alias("sqft_avg_6_dense_rank"),
        F.avg("prev_seq_no").over(w7).alias("prev_seq_no_avg_3_prior"),
        F.avg("sqft").over(w8).alias("city_avg_sqft"),
        F.sum("running_sqft_cume_dist").over(w9).alias("running_running_sqft_cume_dist"),
    )
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .withColumn("seq_no_2", F.row_number().over(w10))
    .withColumn("row_seq", F.row_number().over(w11))
    .withColumn("seq_no_bucket", F.ntile(5).over(w12))
    .withColumn("region_count_city_avg_sqft", F.count("city_avg_sqft").over(w13))
)
