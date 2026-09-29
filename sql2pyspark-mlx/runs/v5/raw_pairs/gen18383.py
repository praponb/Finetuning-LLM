from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w_recent = Window.partitionBy("region").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy(F.col("sqft").desc_nulls_last())

valid = (
    df_stores
    .filter(F.col("opened_date") >= F.lit("2023-08-24").cast("date"))
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        F.coalesce("sqft", F.lit(0)).alias("sqft_filled"),
        F.initcap(F.trim("city")).alias("city_title"),
        F.coalesce("region", F.lit("N/A")).alias("region_filled"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") < F.col("s.opened_date")), "left_semi")
)

deduped = (
    kept
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    deduped
    .withColumn("running_min_sqft_filled", F.min("sqft_filled").over(w_recent))
    .withColumn("running_max_sqft_filled", F.max("sqft_filled").over(w_recent))
    .withColumn("running_count_sqft_filled", F.count("sqft_filled").over(w_recent))
    .withColumn("running_avg_sqft_filled", F.avg("sqft_filled").over(w_recent))
    .withColumn("sqft_percent_rank", F.percent_rank().over(w_running))
)

result = (
    enriched
    .rollup(
        F.when(F.col("sqft") >= 12768, "LARGE").otherwise("SMALL").alias("sqft_tier"),
        "region",
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
    )
    .agg(
        F.count(F.when(F.col("region") == "West", 1)).alias("west_rows"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
    )
    .orderBy(F.col("sqft_tier").desc_nulls_last())
)
