from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("region", "city").orderBy("sqft", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("region", "city")
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("sqft_share_tier").orderBy(F.col("east_last4_sum_sqft").desc())

not_flagged = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") >= F.col("s.opened_date")), "left_anti")
)

valid = (
    not_flagged
    .filter((F.col("opened_date") > F.lit("2023-09-11").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.col("sqft").cast("string").alias("sqft_num"),
        F.concat_ws("-", "city", "store_name").alias("city_store_name_key"),
    )
)

scored = (
    valid
    .withColumn("last4_avg_sqft", F.avg("sqft").over(w_hist))
    .withColumn("last4_count_sqft", F.count("sqft").over(w_hist))
    .withColumn("last4_min_sqft", F.min("sqft").over(w_hist))
    .withColumn("last4_sum_sqft", F.sum("sqft").over(w_hist))
)

matched_keys = (
    scored
    .select("region", "city")
    .exceptAll(
        df_stores
        .filter(F.col("sqft") < 7895)
        .select("region", "city")
    )
)

verified = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "region",
        "city_store_name_key",
        "last4_count_sqft",
        "last4_sum_sqft",
        F.round((F.col("last4_avg_sqft") / F.sum("last4_avg_sqft").over(w2)), 4).alias("last4_avg_sqft_share"),
        F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4).alias("sqft_share"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("sqft_share") >= 0.78, "HIGH").otherwise("LOW").alias("sqft_share_tier"),
        "region",
    )
    .agg(
        F.sum(F.when(F.col("region") == "East", F.col("last4_count_sqft")).otherwise(0)).alias("east_last4_count_sqft"),
        F.sum(F.when(F.col("region") == "East", F.col("last4_sum_sqft")).otherwise(0)).alias("east_last4_sum_sqft"),
        F.round(F.avg("last4_avg_sqft_share"), 2).alias("avg_last4_avg_sqft_share"),
        F.round(F.avg("last4_sum_sqft"), 2).alias("avg_last4_sum_sqft"),
        F.countDistinct("region").alias("distinct_regions"),
    )
    .filter(F.col("avg_last4_sum_sqft") < 1000000)
    .withColumn("east_last4_sum_sqft_rank", F.rank().over(w4))
    .orderBy(F.col("distinct_regions").desc_nulls_last())
)
