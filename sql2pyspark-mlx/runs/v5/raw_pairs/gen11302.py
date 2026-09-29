from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(-6, -1)
w2 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("rating").desc(), "id")
w4 = Window.orderBy("rating_bucket", "id")
w5 = Window.orderBy("cumulative_rating_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "rating",
        F.concat("country", F.lit(" - "), "city").alias("country_city"),
        F.coalesce("city", F.lit("N/A")).alias("city_clean"),
    )
)

enriched = (
    base
    .filter(F.col("supplier_name").like("%Imp%"))
    .select(
        "*",
        F.avg("rating").over(w).alias("rating_avg_6_prior"),
        F.sum("rating").over(w2).alias("cumulative_rating"),
        F.ntile(4).over(w3).alias("rating_bucket"),
        F.sum("rating").over(w2).alias("cumulative_rating_2"),
    )
)

cleaned = (
    enriched
    .filter(F.col("rating_avg_6_prior") > 3)
    .select(
        "id",
        "supplier_name",
        "rating",
        "country_city",
        "city_clean",
        "rating_avg_6_prior",
        "cumulative_rating",
        "rating_bucket",
        "cumulative_rating_2",
        F.when(F.col("cumulative_rating") >= 17, "High").when(F.col("cumulative_rating") >= 9, "Medium").otherwise("Low").alias("cumulative_rating_level"),
    )
)

scored = (
    cleaned
    .withColumn("prev_rating_bucket", F.lag("rating_bucket").over(w4))
    .withColumn("next_cumulative_rating", F.lead("cumulative_rating").over(w4))
    .withColumn("prev_cumulative_rating", F.lag("cumulative_rating").over(w4))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("last_known_rating_bucket", F.last("rating_bucket", ignorenulls=True).over(w5))
)

cleaned_suppliers = (
    scored
    .filter(F.col("cumulative_rating_2") <= 11)
    .select(
        "id",
        "supplier_name",
        "country_city",
        "city_clean",
        "rating_avg_6_prior",
        "cumulative_rating",
        "rating_bucket",
        "cumulative_rating_level",
        "next_cumulative_rating",
        "prev_cumulative_rating",
        "last_known_rating_bucket",
        F.coalesce("prev_cumulative_rating", F.lit(0)).alias("prev_cumulative_rating_filled"),
        F.round("prev_cumulative_rating", 2).alias("prev_cumulative_rating_rounded"),
    )
)

summary = (
    cleaned_suppliers
    .withColumn("prev_cumulative_rating_rounded_tier", F.when(F.col("prev_cumulative_rating_rounded") >= 77, "PREMIUM").otherwise("STANDARD"))
    .withColumn("last_known_rating_bucket_tier", F.when(F.col("last_known_rating_bucket") >= 9, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["prev_cumulative_rating_rounded_tier", "last_known_rating_bucket_tier"], ["prev_cumulative_rating_rounded_tier"]],
        "prev_cumulative_rating_rounded_tier",
        "last_known_rating_bucket_tier",
    )
    .agg(
        F.percentile_approx("prev_cumulative_rating", 0.9).alias("prev_cumulative_rating_p90"),
        F.percentile_approx("last_known_rating_bucket", 0.5).alias("last_known_rating_bucket_p50"),
        F.round(F.avg("last_known_rating_bucket"), 2).alias("avg_last_known_rating_bucket"),
        F.round(F.avg("prev_cumulative_rating"), 2).alias("avg_prev_cumulative_rating"),
    )
    .filter(F.col("avg_last_known_rating_bucket") > 0)
)

result = (
    summary
    .select(
        "last_known_rating_bucket_tier",
        "prev_cumulative_rating_p90",
        "last_known_rating_bucket_p50",
        "avg_last_known_rating_bucket",
        "avg_prev_cumulative_rating",
    )
    .orderBy("prev_cumulative_rating_p90")
)
