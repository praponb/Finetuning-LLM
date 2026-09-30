from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc())
w2 = Window.partitionBy("city", "region").orderBy(F.col("sqft").desc(), "id")

enriched = (
    df_stores
    .select(
        "*",
        F.dense_rank().over(w).alias("sqft_dense_rank"),
        F.ntile(5).over(w2).alias("sqft_bucket"),
    )
)

result = (
    enriched
    .rollup("region", F.date_trunc("MONTH", "opened_date").alias("activity_month"))
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("sqft_dense_rank", [0.25, 0.5, 0.75]).alias("sqft_dense_rank_quartiles"),
        F.count_if(F.col("region") == "East").alias("east_events"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
)
