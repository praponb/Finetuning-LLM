from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("sqft")
w2 = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("region")
w4 = Window.partitionBy("region").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("region").orderBy("first_known_sqft", "id")
w6 = Window.orderBy(F.col("sqft_share").desc(), "id")
w7 = Window.partitionBy("city").orderBy("dedup_rank", "id")
w8 = Window.partitionBy("city").orderBy(F.col("first_known_sqft_bucket").desc(), "id")
w_running = Window.orderBy("first_known_sqft_bucket", "id")
w10 = Window.partitionBy("city").orderBy("first_known_sqft", "id")

ranked = (
    df_stores
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    ranked
    .select(
        "*",
        F.sum("sqft").over(w2).alias("cumulative_sqft"),
        F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4).alias("sqft_share"),
        F.first("sqft", ignorenulls=True).over(w4).alias("first_known_sqft"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.ntile(10).over(w5).alias("first_known_sqft_bucket"),
        F.ntile(4).over(w6).alias("sqft_share_bucket"),
        F.lead("cumulative_sqft", 2).over(w7).alias("next_cumulative_sqft"),
    )
)

valid = (
    enriched
    .filter((F.col("opened_date") > F.to_date(F.lit("2024-02-28"))) & (F.col("cumulative_sqft") != 54529))
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "dedup_rank",
        "sqft_share",
        "first_known_sqft",
        "first_known_sqft_bucket",
        "sqft_share_bucket",
        "next_cumulative_sqft",
        F.concat("city", F.lit(", "), "store_name").alias("city_store_name"),
        F.coalesce("region", F.lit("OTHER")).alias("region_clean"),
    )
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w8))
    .filter(F.col("rank_in_group") == 1)
)

with_windows_stores = (
    latest
    .withColumn("next_next_cumulative_sqft", F.lead("next_cumulative_sqft", 1).over(w_running))
    .withColumn("row_seq", F.row_number().over(w10))
    .withColumn("row_seq_2", F.row_number().over(w8))
)

result = (
    with_windows_stores
    .withColumn("year_start", F.date_trunc("year", "opened_date"))
    .cube("city", "year_start")
    .agg(
        F.avg("sqft_share").alias("avg_sqft_share"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("year_start").asc_nulls_first())
)
