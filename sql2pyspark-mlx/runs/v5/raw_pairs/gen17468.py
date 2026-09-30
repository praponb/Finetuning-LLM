from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_first())
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("region", "city").orderBy("city_avg_sqft", "id")
w_ordered = Window.orderBy(F.col("sqft_cume_dist").desc(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("region", "city").orderBy(F.col("city_avg_sqft").desc_nulls_last(), "id")
w7 = Window.partitionBy("region").orderBy("event_seq", "id").rowsBetween(-2, -1)
w8 = Window.partitionBy("region", "city")
w9 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("region").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("sqft_cume_dist_bucket", "id")

enriched = (
    df_stores
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.cume_dist().over(w).alias("sqft_cume_dist"),
        F.avg("sqft").over(w2).alias("city_avg_sqft"),
    )
)

enriched_stores = (
    enriched
    .filter(F.col("city") == "New York")
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("sqft_cume_dist_bucket", F.ntile(5).over(w_ordered))
)

with_last = (
    enriched_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock"),
    )
)

ranked = (
    with_last
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

enriched_stores_2 = (
    ranked
    .withColumn("event_seq_avg_2_prior", F.avg("event_seq").over(w7))
    .withColumn("region_stddev_samp_city_avg_sqft", F.stddev_samp("city_avg_sqft").over(w8))
    .withColumn("first_known_region", F.first("region", ignorenulls=True).over(w9))
    .withColumn("first_known_sqft_cume_dist_bucket", F.first("sqft_cume_dist_bucket", ignorenulls=True).over(w10))
    .withColumn("sqft_cume_dist_bucket_bucket", F.ntile(4).over(w11))
)

kept_keys = (
    enriched_stores_2
    .select("region", "city")
    .subtract(
        df_stores
        .filter(F.col("sqft") > 12845)
        .select("region", "city")
    )
)

kept_rows = (
    enriched_stores_2
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("quarter_start", F.date_trunc("QUARTER", "last_restock"))
    .withColumn("sqft_cume_dist_bucket_tier", F.when(F.col("sqft_cume_dist_bucket") >= 8, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["quarter_start", "sqft_cume_dist_bucket_tier"], ["quarter_start"]],
        "quarter_start",
        "sqft_cume_dist_bucket_tier",
    )
    .agg(
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_rows"),
        F.round(F.sum("inventory_quantity") / F.nullif(F.count(F.when(F.col("region") == "North", 1)), F.lit(0)), 2).alias("inventory_quantity_per_north"),
        F.round(F.sum("city_avg_sqft") / F.nullif(F.count(F.when(F.col("city") == "Chiang Mai", 1)), F.lit(0)), 2).alias("city_avg_sqft_per_chiang_mai"),
    )
)
