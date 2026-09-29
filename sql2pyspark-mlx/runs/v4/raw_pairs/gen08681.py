from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("country").orderBy("rating")
w3 = Window.partitionBy("city", "country").orderBy(F.col("rating_rank").desc_nulls_last(), "id")
w4 = Window.orderBy(F.col("rating_avg_2_prior").desc(), "id")
w_running = Window.partitionBy("city").orderBy("rating_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city", "country")
w7 = Window.orderBy("rating_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("country")
w9 = Window.partitionBy("country").orderBy(F.col("rating_avg_2_prior").desc(), "id")
w10 = Window.partitionBy("country").orderBy("last_known_rating_avg_2_prior", "id").rowsBetween(-6, -1)
w11 = Window.partitionBy("country").orderBy(F.col("city_avg_rating").asc_nulls_last())
w12 = Window.partitionBy("city")

base = (
    df_suppliers
    .filter(F.col("city").isin(["Chiang Mai", "New York"]))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.concat("city", F.lit("/"), "supplier_name").alias("city_supplier_name"),
    )
)

with_windows = (
    base
    .withColumn("rating_avg_2_prior", F.avg("rating").over(w))
    .withColumn("rating_rank", F.rank().over(w2))
)

ranked = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_windows_suppliers = (
    ranked
    .select(
        "*",
        F.ntile(3).over(w4).alias("rating_avg_2_prior_bucket"),
        F.last("rating_avg_2_prior", ignorenulls=True).over(w_running).alias("last_known_rating_avg_2_prior"),
        F.avg("rating").over(w6).alias("city_avg_rating"),
        F.last("rating", ignorenulls=True).over(w_running).alias("last_known_rating"),
        F.sum("rating_rank").over(w7).alias("running_rating_rank"),
    )
)

with_windows_suppliers_2 = (
    with_windows_suppliers
    .withColumn("rating_rank_share", F.round((F.col("rating_rank") / F.sum("rating_rank").over(w8)), 4))
    .withColumn("rating_avg_2_prior_bucket_2", F.ntile(4).over(w9))
    .withColumn("rating_rank_avg_6_prior", F.avg("rating_rank").over(w10))
    .withColumn("city_avg_rating_rank", F.rank().over(w11))
)

matched_keys = (
    with_windows_suppliers_2
    .select("country")
    .intersectAll(
        df_suppliers
        .filter(F.col("rating").isNotNull())
        .select("country")
    )
)

reconciled = (
    with_windows_suppliers_2
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

with_windows_suppliers_3 = (
    reconciled
    .select(
        "*",
        F.max("last_known_rating_avg_2_prior").over(w8).alias("country_max_last_known_rating_avg_2_prior"),
        F.stddev_samp("city_avg_rating_rank").over(w12).alias("city_stddev_samp_city_avg_rating_rank"),
    )
)

result = (
    with_windows_suppliers_3
    .select(
        "country",
        "city",
        "rating",
        "rating_rank",
        "rating_avg_2_prior_bucket",
        "city_avg_rating",
        "running_rating_rank",
        "rating_rank_share",
        "city_avg_rating_rank",
    )
)
