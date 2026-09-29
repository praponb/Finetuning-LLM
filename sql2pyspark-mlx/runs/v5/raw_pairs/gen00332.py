from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("city", "region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region", "city").orderBy(F.col("running_count_sqft").desc_nulls_last(), "id")
w3 = Window.partitionBy("region")
w4 = Window.partitionBy("region").orderBy("running_max_sqft", "id").rowsBetween(-5, Window.currentRow)

eligible = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") < F.col("s.opened_date")), "left_anti")
)

scored = (
    eligible
    .select(
        "id",
        "city",
        "region",
        "sqft",
        F.avg("sqft").over(w_recent).alias("running_avg_sqft"),
        F.count("sqft").over(w_recent).alias("running_count_sqft"),
        F.min("sqft").over(w_recent).alias("running_min_sqft"),
        F.max("sqft").over(w_recent).alias("running_max_sqft"),
    )
)

deduped = (
    scored
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "region",
        "running_avg_sqft",
        "running_count_sqft",
        "running_min_sqft",
        "running_max_sqft",
    )
)

with_windows = (
    deduped
    .select(
        "id",
        "running_avg_sqft",
        F.sum("running_max_sqft").over(w3).alias("region_sum_running_max_sqft"),
        F.avg("running_avg_sqft").over(w4).alias("running_avg_sqft_avg_5"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("running_avg_sqft_avg_5") >= 12789, "LARGE").otherwise("SMALL").alias("running_avg_sqft_avg_5_tier"),
    )
    .agg(
        F.min("region_sum_running_max_sqft").alias("min_region_sum_running_max_sqft"),
        F.percentile_approx("running_avg_sqft_avg_5", 0.9).alias("running_avg_sqft_avg_5_p90"),
        F.percentile_approx("region_sum_running_max_sqft", [0.25, 0.5, 0.75]).alias("region_sum_running_max_sqft_quartiles"),
    )
    .orderBy(F.col("running_avg_sqft_avg_5_p90").desc_nulls_last())
)
