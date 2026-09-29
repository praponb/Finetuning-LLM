from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("city").orderBy(F.col("sqft").asc_nulls_first())

cleaned = (
    df_stores
    .filter(F.col("opened_date") >= "2024-12-01")
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.concat_ws(",", "city", "store_name").alias("city_store_name_key"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("resolved_region"),
    )
)

scored = (
    cleaned
    .withColumn("city_stddev_samp_sqft", F.stddev_samp("sqft").over(w))
    .withColumn("sqft_dense_rank", F.dense_rank().over(w2))
)

base = (
    scored
    .filter((F.col("opened_date") > F.lit("2023-09-12").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "city_store_name_key",
        "resolved_region",
        "city_stddev_samp_sqft",
        F.date_sub("opened_date", 5).alias("opened_date_minus_5d"),
    )
)

base_stores = (
    base
    .filter((F.col("opened_date") > F.lit("2023-03-06").cast("date")) & (F.col("city") == "Berlin"))
    .withColumn("sqft_whole", F.floor("sqft"))
    .withColumn("region_filled", F.coalesce("region", F.lit("DIRECT")))
)

kept_keys = (
    base_stores
    .select("region", "id")
    .intersectAll(
        df_stores
        .filter(F.col("region") == "West")
        .select("region", "id")
    )
)

verified = (
    base_stores
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") < F.col("s.opened_date")), "left_anti")
)

daily = (
    eligible
    .groupBy("city")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("sqft_whole", 0.9).alias("sqft_whole_p90"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("region") == "East", 1)), F.lit(0)), 2).alias("sqft_per_east"),
        F.percentile_approx("sqft_whole", 0.9).alias("sqft_whole_p90_2"),
    )
    .filter(F.col("distinct_ids") >= 1)
)

result = (
    daily
    .select("city", "distinct_ids", "sqft_per_east")
    .orderBy(F.col("sqft_per_east").desc_nulls_first())
)
