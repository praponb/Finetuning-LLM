from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.orderBy("running_count_sqft", "id")
w4 = Window.partitionBy("city", "region").orderBy("opened_date", "id")
w_running = Window.orderBy("running_max_sqft", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.partitionBy("region").orderBy("sqft_avg_6", "id")
w_grp = Window.partitionBy("city").orderBy("next_sqft_avg_6", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

cleaned = (
    df_stores
    .filter(F.col("sqft") > 11901)
    .withColumn("store_name_slug", F.regexp_replace("store_name", " ", ""))
    .withColumn("region_filled", F.coalesce("region", F.lit("UNKNOWN")))
)

scored = (
    cleaned
    .select(
        "*",
        F.avg("sqft").over(w).alias("running_avg_sqft"),
        F.min("sqft").over(w).alias("running_min_sqft"),
        F.max("sqft").over(w).alias("running_max_sqft"),
        F.count("sqft").over(w).alias("running_count_sqft"),
        F.avg("sqft").over(w2).alias("sqft_avg_6"),
    )
)

scored_stores = (
    scored
    .filter(F.col("sqft_avg_6") >= 9971)
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "store_name_slug",
        "running_min_sqft",
        "running_max_sqft",
        "running_count_sqft",
        F.lag("running_count_sqft", 2, 0).over(w3).alias("prev_running_count_sqft"),
        F.lead("sqft_avg_6", 1, 0).over(w4).alias("next_sqft_avg_6"),
        F.avg("running_max_sqft").over(w_running).alias("running_max_sqft_avg_2"),
        F.lead("running_min_sqft", 1).over(w6).alias("next_running_min_sqft"),
        F.first("running_count_sqft", ignorenulls=True).over(w).alias("first_known_running_count_sqft"),
    )
)

with_windows = (
    scored_stores
    .select(
        "id",
        "opened_date",
        "store_name_slug",
        "running_min_sqft",
        "running_max_sqft",
        "next_sqft_avg_6",
        "running_max_sqft_avg_2",
        F.count("prev_running_count_sqft").over(w_grp).alias("last3_count_prev_running_count_sqft"),
        F.min("prev_running_count_sqft").over(w_grp).alias("last3_min_prev_running_count_sqft"),
        F.avg("prev_running_count_sqft").over(w_grp).alias("last3_avg_prev_running_count_sqft"),
        F.sum("prev_running_count_sqft").over(w_grp).alias("last3_sum_prev_running_count_sqft"),
    )
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.opened_date",
        "s.running_min_sqft",
        "s.running_max_sqft",
        "s.running_max_sqft_avg_2",
        "s.last3_min_prev_running_count_sqft",
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

result = (
    with_latest
    .withColumn("inventory_quantity_tier", F.when(F.col("inventory_quantity") >= 208, "HIGH").otherwise("LOW"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "opened_date"))
    .rollup("inventory_quantity_tier", "quarter_start")
    .agg(
        F.percentile_approx("running_min_sqft", [0.25, 0.5, 0.75]).alias("running_min_sqft_quartiles"),
        F.percentile_approx("running_min_sqft", [0.25, 0.5, 0.75]).alias("running_min_sqft_quartiles_2"),
        F.round(F.avg("running_min_sqft"), 2).alias("avg_running_min_sqft"),
        F.percentile_approx("running_min_sqft", 0.5).alias("running_min_sqft_p50"),
        F.count("*").alias("num_rows"),
        F.grouping("inventory_quantity_tier").alias("grouping_id"),
    )
    .filter(F.col("num_rows") >= 1)
)
