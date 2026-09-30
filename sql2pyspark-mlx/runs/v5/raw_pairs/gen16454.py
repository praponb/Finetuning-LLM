from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("region").orderBy("running_sqft", "id")
w_hist = Window.partitionBy("region")
w6 = Window.partitionBy("region").orderBy("next_first_known_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("first_known_sqft", "id")
w8 = Window.orderBy("region_avg_running_sqft", "id")
w9 = Window.orderBy("sqft", "id")
w10 = Window.orderBy(F.col("event_seq_2").desc_nulls_last())
w11 = Window.orderBy("running_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.orderBy(F.col("prev_running_sqft").desc(), "id")

metrics = (
    df_stores
    .select(
        "id",
        "store_name",
        "region",
        "sqft",
        F.first("sqft", ignorenulls=True).over(w).alias("first_known_sqft"),
        F.sum("sqft").over(w2).alias("running_sqft"),
        F.min("sqft").over(w3).alias("city_min_sqft"),
    )
)

metrics_stores = (
    metrics
    .filter(F.col("sqft") <= 5097)
    .select(
        "id",
        "region",
        "sqft",
        "first_known_sqft",
        "running_sqft",
        F.lead("first_known_sqft").over(w4).alias("next_first_known_sqft"),
        F.lag("running_sqft").over(w4).alias("prev_running_sqft"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

enriched = (
    metrics_stores
    .select(
        "id",
        "sqft",
        "first_known_sqft",
        "running_sqft",
        "next_first_known_sqft",
        "prev_running_sqft",
        "event_seq",
        F.avg("running_sqft").over(w_hist).alias("region_avg_running_sqft"),
        F.min("prev_running_sqft").over(w_hist).alias("region_min_prev_running_sqft"),
        F.sum("sqft").over(w6).alias("cumulative_sqft"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.lead("region_avg_running_sqft").over(w7).alias("next_region_avg_running_sqft"),
        F.lead("sqft").over(w7).alias("next_sqft"),
        F.lag("sqft").over(w7).alias("prev_sqft"),
        F.row_number().over(w7).alias("event_seq_2"),
        F.lead("region_min_prev_running_sqft", 2).over(w8).alias("next_region_min_prev_running_sqft"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.lead("region_avg_running_sqft", 2).over(w9).alias("next_region_avg_running_sqft_2"),
        F.cume_dist().over(w10).alias("event_seq_2_cume_dist"),
        F.last("next_region_min_prev_running_sqft", ignorenulls=True).over(w11).alias("last_known_next_region_min_prev_running_sqft"),
        F.row_number().over(w12).alias("seq_no"),
    )
)

result = (
    with_windows
    .cube(
        F.when(F.col("last_known_next_region_min_prev_running_sqft") >= 74209, "PREMIUM").otherwise("STANDARD").alias("last_known_next_region_min_prev_running_sqft_tier"),
        F.when(F.col("event_seq_2_cume_dist") >= 0.81, "PREMIUM").otherwise("STANDARD").alias("event_seq_2_cume_dist_tier"),
        "seq_no",
    )
    .agg(
        F.round(F.avg("running_sqft"), 2).alias("avg_running_sqft"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.max("next_region_avg_running_sqft").alias("max_next_region_avg_running_sqft"),
    )
    .orderBy(F.col("avg_running_sqft").asc_nulls_last())
)
