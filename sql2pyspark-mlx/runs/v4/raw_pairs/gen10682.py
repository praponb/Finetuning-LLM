from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_first(), "id")
w_running = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_last())
w3 = Window.partitionBy("region")
w4 = Window.partitionBy("region").orderBy(F.col("sqft").asc_nulls_last())
w5 = Window.partitionBy("city")
w_hist = Window.partitionBy("region", "city").orderBy(F.col("sqft_rank").desc_nulls_first())

not_flagged = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.store_id").isin([1, 2, 4])), "left_semi")
)

ranked = (
    not_flagged
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

matched_keys = (
    ranked
    .select("id", "region")
    .subtract(
        df_stores
        .filter(F.col("sqft") <= 5604)
        .select("id", "region")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .withColumn("sqft_rank", F.rank().over(w_running))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4))
    .withColumn("sqft_dense_rank", F.dense_rank().over(w4))
)

scored_stores = (
    scored
    .withColumn("sqft_rank_2", F.rank().over(w_running))
    .withColumn("city_stddev_samp_sqft_rank", F.stddev_samp("sqft_rank").over(w5))
    .withColumn("sqft_rank_percent_rank", F.percent_rank().over(w_hist))
)

base = (
    scored_stores
    .filter(F.col("sqft_rank_2") < 7)
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "rank_in_group",
        "sqft_rank",
        "sqft_share",
        "sqft_dense_rank",
        "sqft_rank_2",
        "city_stddev_samp_sqft_rank",
        "sqft_rank_percent_rank",
        (F.col("city_stddev_samp_sqft_rank") - F.col("rank_in_group")).alias("city_stddev_samp_sqft_rank_minus_rank_in_group"),
        F.col("rank_in_group").cast("string").alias("rank_in_group_num"),
        F.coalesce("region", F.lit("N/A")).alias("region_filled"),
    )
)

summary = (
    base
    .groupBy(
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
        F.date_trunc("MONTH", "opened_date").alias("activity_month"),
    )
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.count(F.when(F.col("region") == "West", 1)).alias("west_count"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
    )
    .filter(F.col("central_count") >= 1)
)

result = (
    summary
    .select("week_start", "activity_month", "west_count")
    .orderBy(F.col("activity_month").desc())
)
