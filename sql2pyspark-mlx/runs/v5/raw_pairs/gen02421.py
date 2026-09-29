from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-2, Window.currentRow)
w_recent = Window.partitionBy("city")
w_ordered = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_first())

enriched = (
    df_suppliers
    .select(
        "*",
        F.avg("rating").over(w).alias("rating_avg_2"),
        F.stddev_samp("rating").over(w_recent).alias("city_stddev_samp_rating"),
        F.dense_rank().over(w_ordered).alias("rating_dense_rank"),
    )
)

result = (
    enriched
    .groupBy("city")
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.sum(F.when(F.col("country") == "TH", F.col("rating_dense_rank")).otherwise(0)).alias("th_rating_dense_rank"),
        F.round(F.sum("rating_dense_rank") / F.nullif(F.count(F.when(F.col("city") == "Berlin", 1)), F.lit(0)), 2).alias("rating_dense_rank_per_berlin"),
    )
    .orderBy("th_rating_dense_rank")
)
