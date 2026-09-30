from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id")
w2 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("rating", "id")
w4 = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w5 = Window.partitionBy("country").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("country").orderBy("rating_bucket", "id")
w7 = Window.partitionBy("city").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("city")
w9 = Window.partitionBy("city").orderBy("last_known_rating_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("country")
w11 = Window.orderBy("rating_bucket", "id").rowsBetween(-3, Window.currentRow)
w12 = Window.orderBy("country_min_running_rating", "id").rowsBetween(-2, Window.currentRow)
w13 = Window.orderBy("last_known_rating_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_suppliers
    .select(
        "*",
        F.lag("rating", 1, 0).over(w).alias("prev_rating"),
        F.sum("rating").over(w2).alias("running_rating"),
        F.row_number().over(w3).alias("event_seq"),
        F.ntile(3).over(w4).alias("rating_bucket"),
    )
)

with_windows_suppliers = (
    with_windows
    .select(
        "*",
        F.last("rating_bucket", ignorenulls=True).over(w5).alias("last_known_rating_bucket"),
        F.row_number().over(w6).alias("row_seq"),
        F.first("event_seq", ignorenulls=True).over(w7).alias("first_known_event_seq"),
        F.count("rating").over(w8).alias("city_count_rating"),
    )
)

with_windows_suppliers_2 = (
    with_windows_suppliers
    .select(
        "*",
        F.first("city", ignorenulls=True).over(w9).alias("first_known_city"),
        F.min("running_rating").over(w_hist).alias("country_min_running_rating"),
    )
)

scored = (
    with_windows_suppliers_2
    .withColumn("rating_bucket_avg_3", F.avg("rating_bucket").over(w11))
    .withColumn("country_min_running_rating_avg_2", F.avg("country_min_running_rating").over(w12))
    .withColumn("event_seq_2", F.row_number().over(w6))
    .withColumn("cumulative_row_seq", F.sum("row_seq").over(w13))
)

result = (
    scored
    .groupBy(
        "country",
        F.when(F.col("last_known_rating_bucket") >= 9, "HIGH").otherwise("LOW").alias("last_known_rating_bucket_tier"),
    )
    .agg(
        F.percentile_approx("prev_rating", [0.25, 0.5, 0.75]).alias("prev_rating_quartiles"),
        F.percentile_approx("row_seq", 0.5).alias("row_seq_p50"),
        F.percentile_approx("running_rating", [0.25, 0.5, 0.75]).alias("running_rating_quartiles"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.round(F.sum("event_seq_2") / F.nullif(F.count(F.when(F.col("country") == "DE", 1)), F.lit(0)), 2).alias("event_seq_2_per_de"),
        F.sum(F.when(F.col("country") == "SG", F.col("city_count_rating")).otherwise(0)).alias("sg_city_count_rating"),
    )
    .filter(F.col("sg_city_count_rating") < 0)
    .orderBy("rating_stddev")
)
