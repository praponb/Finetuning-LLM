from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city")
w2 = Window.partitionBy("region", "city").orderBy("opened_date_month", "id")
w3 = Window.partitionBy("city", "region").orderBy(F.col("opened_date_month").desc_nulls_last())
w4 = Window.partitionBy("city").orderBy(F.col("opened_date_month").desc(), "id")
w5 = Window.partitionBy("city").orderBy(F.col("opened_date_month").desc())

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.month("opened_date").alias("opened_date_month"),
        F.coalesce("region", F.lit("OTHER")).alias("region_clean"),
    )
)

kept = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.quantity").between(130, 236)), "left_semi")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.month(F.col("ch.last_restock")).isin([3, 4, 9])), "left_anti")
)

metrics = (
    not_flagged
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "opened_date_month",
        F.max("opened_date_month").over(w).alias("region_max_opened_date_month"),
        F.ntile(5).over(w2).alias("opened_date_month_bucket"),
        F.rank().over(w3).alias("opened_date_month_rank"),
        F.ntile(10).over(w4).alias("opened_date_month_bucket_2"),
        F.percent_rank().over(w5).alias("opened_date_month_percent_rank"),
    )
)

valid = (
    metrics
    .filter((F.col("opened_date") > F.lit("2023-07-14").cast("date")) & (F.col("region") == "West"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "region_max_opened_date_month",
        "opened_date_month_bucket",
        "opened_date_month_rank",
        "opened_date_month_bucket_2",
        "opened_date_month_percent_rank",
        F.col("opened_date_month_bucket").cast("double").alias("opened_date_month_bucket_num"),
        F.coalesce("region", F.lit("DIRECT")).alias("region_filled"),
    )
)

result = (
    valid
    .groupBy(
        F.date_trunc("year", "opened_date").alias("year_start"),
        F.when(F.col("opened_date_month_percent_rank") >= 0.62, "HIGH").otherwise("LOW").alias("opened_date_month_percent_rank_tier"),
    )
    .agg(
        F.sum("opened_date_month_percent_rank").alias("total_opened_date_month_percent_rank"),
        F.count("*").alias("event_count"),
        F.countDistinct("region").alias("distinct_regions"),
        F.countDistinct("region").alias("distinct_regions_2"),
        F.count_if(F.col("city") == "New York").alias("new_york_events"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
    )
)
