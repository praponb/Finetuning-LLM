from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("country")
w3 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_suppliers
    .select(
        "*",
        F.avg("rating").over(w).alias("rating_avg_2_prior"),
        F.stddev_samp("rating").over(w2).alias("country_stddev_samp_rating"),
        F.sum("rating").over(w3).alias("cumulative_rating"),
        F.round((F.col("rating") / F.sum("rating").over(w2)), 4).alias("rating_share"),
        F.last("city", ignorenulls=True).over(w4).alias("last_known_city"),
    )
)

result = (
    scored
    .rollup(
        "country",
        F.when(F.col("country_stddev_samp_rating") >= 4, "HIGH").otherwise("LOW").alias("country_stddev_samp_rating_tier"),
    )
    .agg(
        F.countDistinct("country").alias("distinct_countries"),
        F.round(F.sum("rating_avg_2_prior") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("rating_avg_2_prior_per_singapore"),
        F.round(F.sum("rating_avg_2_prior") / F.nullif(F.count(F.when(F.col("country") == "TH", 1)), F.lit(0)), 2).alias("rating_avg_2_prior_per_th"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("rating_avg_2_prior"))).alias("berlin_total_rating_avg_2_prior"),
        F.round(F.avg("cumulative_rating"), 2).alias("avg_cumulative_rating"),
    )
    .filter(F.col("avg_cumulative_rating") > 0)
    .orderBy(F.col("rating_avg_2_prior_per_th").desc_nulls_last())
)
