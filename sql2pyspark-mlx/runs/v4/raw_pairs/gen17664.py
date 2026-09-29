from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.orderBy(F.col("rating").asc_nulls_last())
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("city").orderBy("rating_cume_dist", "id")
w5 = Window.partitionBy("city").orderBy("is_us", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("is_bangkok", "id")
w7 = Window.partitionBy("city", "country").orderBy("rating_cume_dist", "id")
w8 = Window.partitionBy("city").orderBy(F.col("rating_share").desc(), "id")
w9 = Window.orderBy("city_sum_next_rating_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("country", "city").orderBy("is_bangkok_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.when(F.col("country") == "US", 1).otherwise(0).alias("is_us"),
        F.when(F.col("city") == "Bangkok", 1).otherwise(0).alias("is_bangkok"),
    )
)

enriched = (
    cleaned
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w)), 4))
    .withColumn("rating_cume_dist", F.cume_dist().over(w2))
)

metrics = (
    enriched
    .select(
        "*",
        F.count("is_us").over(w3).alias("city_count_is_us"),
        F.lead("rating_share", 1).over(w4).alias("next_rating_share"),
        F.sum("is_bangkok").over(w5).alias("cumulative_is_bangkok"),
    )
)

metrics_suppliers = (
    metrics
    .withColumn("is_bangkok_bucket", F.ntile(5).over(w_part))
    .withColumn("city_sum_next_rating_share", F.sum("next_rating_share").over(w3))
)

with_windows = (
    metrics_suppliers
    .select(
        "*",
        F.lag("cumulative_is_bangkok", 1).over(w7).alias("prev_cumulative_is_bangkok"),
        F.ntile(4).over(w8).alias("rating_share_bucket"),
        F.sum("city_count_is_us").over(w9).alias("running_city_count_is_us"),
        F.count("next_rating_share").over(w3).alias("city_count_next_rating_share"),
        F.first("next_rating_share", ignorenulls=True).over(w10).alias("first_known_next_rating_share"),
    )
)

summary = (
    with_windows
    .withColumn("city_sum_next_rating_share_tier", F.when(F.col("city_sum_next_rating_share") >= 3, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["city", "city_sum_next_rating_share_tier"], ["city"]],
        "city",
        "city_sum_next_rating_share_tier",
    )
    .agg(
        F.sum(F.when(F.col("city") == "London", F.col("city_count_is_us")).otherwise(0)).alias("london_city_count_is_us"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.percentile_approx("rating_share_bucket", [0.25, 0.5, 0.75]).alias("rating_share_bucket_quartiles"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = summary.select("de_count", "rating_share_bucket_quartiles", "grouping_level")
