from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("region").orderBy("sqft", "id")
w_ordered = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("region").orderBy(F.col("row_seq").desc(), "id")

base = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-03-25").cast("date")) & F.col("region").isNotNull())
    .select(
        "*",
        F.concat("city", F.lit("/"), "region").alias("city_region"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_clean"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") >= F.col("s.opened_date")), "left_anti")
)

enriched = (
    eligible
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        "city_region",
        F.sum("sqft").over(w).alias("city_sum_sqft"),
        F.last("region", ignorenulls=True).over(w2).alias("last_known_region"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.max("city_sum_sqft").over(w_ordered).alias("running_max_city_sum_sqft"),
        F.min("city_sum_sqft").over(w_ordered).alias("running_min_city_sum_sqft"),
        F.count("city_sum_sqft").over(w_ordered).alias("running_count_city_sum_sqft"),
        F.sum("city_sum_sqft").over(w_ordered).alias("running_sum_city_sum_sqft"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "city",
        "region",
        "city_region",
        "last_known_region",
        "running_max_city_sum_sqft",
        "running_min_city_sum_sqft",
        F.ntile(10).over(w5).alias("row_seq_bucket"),
        F.round((F.col("running_min_city_sum_sqft") / F.sum("running_min_city_sum_sqft").over(w)), 4).alias("running_min_city_sum_sqft_share"),
    )
)

daily = (
    scored
    .groupBy(
        F.when(F.col("running_min_city_sum_sqft_share") >= 0.63, "HIGH").otherwise("LOW").alias("running_min_city_sum_sqft_share_tier"),
    )
    .agg(
        F.round(F.avg("running_min_city_sum_sqft"), 2).alias("avg_running_min_city_sum_sqft"),
        F.round(F.avg("running_min_city_sum_sqft_share"), 2).alias("avg_running_min_city_sum_sqft_share"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_rows"),
    )
)

result = (
    daily
    .select(
        "running_min_city_sum_sqft_share_tier",
        "avg_running_min_city_sum_sqft",
        "berlin_rows",
    )
    .orderBy(F.col("running_min_city_sum_sqft_share_tier").desc())
)
