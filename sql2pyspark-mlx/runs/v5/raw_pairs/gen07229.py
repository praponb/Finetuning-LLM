from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("country_length").desc_nulls_first(), "id")
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w_part = Window.partitionBy("city")
w4 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_last())
w_grp = Window.partitionBy("city").orderBy("country_length_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("city").orderBy(F.col("country_length_avg_2").desc(), "id")

filtered = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Glob%"))
    .select(
        "*",
        F.length("country").alias("country_length"),
        F.trim("country").alias("clean_country"),
        F.coalesce("city", F.lit("NONE")).alias("city_filled"),
    )
)

latest = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "supplier_name", "city", "rating", "country_length")
)

with_windows = (
    latest
    .select(
        "id",
        "city",
        "country_length",
        F.row_number().over(w2).alias("row_seq"),
        F.stddev_samp("rating").over(w_part).alias("city_stddev_samp_rating"),
        F.avg("country_length").over(w4).alias("country_length_avg_2"),
        F.cume_dist().over(w5).alias("rating_cume_dist"),
        F.stddev_samp("country_length").over(w_part).alias("city_stddev_samp_country_length"),
    )
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.cost") <= 590), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "row_seq",
        "country_length_avg_2",
        F.count("rating_cume_dist").over(w_grp).alias("running_count_rating_cume_dist"),
        F.sum("rating_cume_dist").over(w_grp).alias("running_sum_rating_cume_dist"),
        F.avg("rating_cume_dist").over(w_grp).alias("running_avg_rating_cume_dist"),
        F.min("rating_cume_dist").over(w_grp).alias("running_min_rating_cume_dist"),
        F.ntile(10).over(w_running).alias("country_length_avg_2_bucket"),
    )
)
