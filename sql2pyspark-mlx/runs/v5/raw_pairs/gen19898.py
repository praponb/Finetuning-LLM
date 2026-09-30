from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("city", "country").orderBy(F.col("rating").desc())
w4 = Window.partitionBy("city").orderBy("rating_avg_3_prior")
w5 = Window.orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city").orderBy(F.col("rating_cume_dist").desc_nulls_first())

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("rating") <= 3.0))
    .withColumn("rating_band", F.when(F.col("rating") >= 4.1, "High").when(F.col("rating") >= 2.3, "Medium").otherwise("Low"))
)

latest = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    latest
    .filter(F.col("latest_rank") <= 1)
    .select(
        "id",
        "city",
        "rating",
        "latest_rank",
        F.avg("rating").over(w2).alias("rating_avg_3_prior"),
        F.cume_dist().over(w3).alias("rating_cume_dist"),
    )
)

enriched = (
    metrics
    .withColumn("rating_avg_3_prior_dense_rank", F.dense_rank().over(w4))
    .withColumn("last_known_rating_avg_3_prior", F.last("rating_avg_3_prior", ignorenulls=True).over(w5))
    .withColumn("rating_cume_dist_cume_dist", F.cume_dist().over(w6))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("rating_cume_dist_cume_dist") >= 0.76, "PREMIUM").otherwise("STANDARD").alias("rating_cume_dist_cume_dist_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count("*").alias("row_count"),
        F.round(F.sum("rating_cume_dist_cume_dist") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("rating_cume_dist_cume_dist_per_new_york"),
    )
)
