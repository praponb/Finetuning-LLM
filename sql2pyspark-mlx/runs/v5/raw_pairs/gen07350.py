from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-5, -1)
w2 = Window.orderBy("sqft", "id")
w3 = Window.partitionBy("region").orderBy(F.col("sqft_avg_5_prior").desc_nulls_first())
w4 = Window.partitionBy("region").orderBy(F.col("sqft_avg_5_prior_minus_sqft").desc_nulls_last())

valid = (
    df_stores
    .filter(F.col("opened_date").between("2025-04-02", "2025-10-16"))
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        F.upper("store_name").alias("store_name_upper"),
    )
)

scored = (
    valid
    .filter(F.col("region") == "West")
    .select(
        "*",
        F.avg("sqft").over(w).alias("sqft_avg_5_prior"),
        F.lead("sqft", 1, 0).over(w2).alias("next_sqft"),
    )
)

filtered = (
    scored
    .filter(~F.col("region").eqNullSafe(F.lit("Central")))
    .withColumn("sqft_avg_5_prior_minus_sqft", (F.col("sqft_avg_5_prior") - F.col("sqft")))
    .withColumn("region_filled", F.coalesce("region", F.lit("UNKNOWN")))
)

kept = (
    filtered
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") <= F.col("s.opened_date")), "left_anti")
)

ranked = (
    kept
    .withColumn("rn", F.rank().over(w3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

latest = (
    ranked
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .cube("region")
    .agg(
        F.sum(F.when(F.col("region") == "West", F.col("next_sqft")).otherwise(0)).alias("west_next_sqft"),
        F.countDistinct("region").alias("distinct_regions"),
        F.percentile_approx("sqft_avg_5_prior", 0.9).alias("sqft_avg_5_prior_p90"),
        F.count("*").alias("num_rows"),
        F.count_if(F.col("region") == "South").alias("south_events"),
        F.grouping("region").alias("grouping_level"),
    )
    .filter(F.col("south_events") > 1)
)
