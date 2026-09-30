from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("region").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("sqft").desc(), "id")
w5 = Window.partitionBy("region").orderBy(F.col("opened_date").asc_nulls_last())
w6 = Window.partitionBy("region").orderBy(F.col("sqft_share").desc(), "id")

filtered = (
    df_stores
    .filter(F.col("region") == "South")
    .withColumn("region_prefix", F.substring("region", 1, 2))
)

deduped = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") >= F.col("s.opened_date")), "left_anti")
)

scored = (
    not_flagged
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        "region_prefix",
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4).alias("sqft_share"),
        F.first("sqft", ignorenulls=True).over(w3).alias("first_known_sqft"),
        F.ntile(4).over(w4).alias("sqft_bucket"),
    )
)

latest = (
    scored
    .withColumn("rank_in_group", F.rank().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

ranked = (
    latest
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .groupBy(
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
        F.date_trunc("YEAR", "opened_date").alias("year_start"),
    )
    .agg(
        F.percentile_approx("sqft_share", [0.25, 0.5, 0.75]).alias("sqft_share_quartiles"),
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.round(F.avg("sqft_share"), 2).alias("avg_sqft_share"),
        F.percentile_approx("first_known_sqft", [0.25, 0.5, 0.75]).alias("first_known_sqft_quartiles"),
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") >= 1)
    .orderBy(F.col("year_start").desc_nulls_last())
)
