from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc())
w2 = Window.partitionBy("region").orderBy("opened_date", "id")
w3 = Window.partitionBy("city", "region")
w4 = Window.partitionBy("city").orderBy("sqft_share", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("region").orderBy("next_sqft", "id")
w6 = Window.partitionBy("city", "region").orderBy("sqft_share", "id")
w7 = Window.partitionBy("next_next_sqft_tier").orderBy(F.col("sqft_stddev").desc())

scored = (
    df_stores
    .withColumn("sqft_dense_rank", F.dense_rank().over(w))
    .withColumn("next_sqft", F.lead("sqft", 1).over(w2))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4))
)

filtered = (
    scored
    .filter(F.col("next_sqft") < 9891)
    .select("*", F.ceil("sqft_dense_rank").alias("sqft_dense_rank_whole"))
)

scored_stores = (
    filtered
    .select(
        "*",
        F.avg("sqft_dense_rank").over(w4).alias("sqft_dense_rank_avg_5_prior"),
        F.ntile(10).over(w5).alias("next_sqft_bucket"),
        F.lead("next_sqft", 1).over(w6).alias("next_next_sqft"),
    )
)

result = (
    scored_stores
    .groupBy(
        F.when(F.col("next_next_sqft") >= 16395, "HIGH").otherwise("LOW").alias("next_next_sqft_tier"),
        F.when(F.col("sqft_dense_rank_avg_5_prior") >= 9, "LARGE").otherwise("SMALL").alias("sqft_dense_rank_avg_5_prior_tier"),
    )
    .agg(
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.round(F.avg("sqft_dense_rank_whole"), 2).alias("avg_sqft_dense_rank_whole"),
    )
    .withColumn("sqft_stddev_rank", F.rank().over(w7))
)
