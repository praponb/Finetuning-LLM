from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("credit_limit").desc_nulls_last())
w_hist = Window.partitionBy("city")
w3 = Window.partitionBy("country")

cleaned = (
    df_customers
    .filter(~F.col("city").isin(["Chiang Mai", "Tokyo", "New York"]))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        F.length("name").alias("name_length"),
    )
)

remaining_keys = (
    cleaned
    .select("id")
    .intersectAll(
        df_customers
        .filter(F.col("name").like("Al%"))
        .select("id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .select(
        "*",
        F.cume_dist().over(w_recent).alias("credit_limit_cume_dist"),
        F.stddev_samp("name_length").over(w_hist).alias("city_stddev_samp_name_length"),
    )
)

valid = (
    metrics
    .filter(F.col("city").isNotNull())
    .withColumn("name_length_filled", F.coalesce("name_length", F.lit(0)))
    .withColumn("name_title", F.initcap(F.trim("name")))
)

base = (
    valid
    .filter(F.col("signup_date") >= F.lit("2023-08-04").cast("date"))
    .select(
        "id",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "name_length",
        "city_stddev_samp_name_length",
        "name_length_filled",
        F.substring("segment", 1, 3).alias("segment_prefix"),
        (F.col("name_length_filled") + F.col("city_stddev_samp_name_length")).alias("name_length_filled_plus_city_stddev_samp_name_length"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "country",
        "name_length",
        "city_stddev_samp_name_length",
        "name_length_filled_plus_city_stddev_samp_name_length",
        "city_filled",
        F.stddev_samp("city_stddev_samp_name_length").over(w3).alias("country_stddev_samp_city_stddev_samp_name_length"),
        F.min("credit_limit").over(w_hist).alias("city_min_credit_limit"),
    )
)

result = (
    with_windows
    .cube("country")
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.sort_array(F.collect_set("country")).alias("country_list_2"),
        F.count_if(F.col("country") == "TH").alias("th_events"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("th_events") > 1)
    .orderBy(F.col("th_events").desc())
)
