from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("region", "city").orderBy(F.col("region_length").desc(), "id")
w4 = Window.partitionBy("region").orderBy(F.col("city_min_region_length").desc(), "id")
w_part = Window.partitionBy("city").orderBy("opened_date", "id")
w6 = Window.partitionBy("region", "city").orderBy("region_length", "id")
w7 = Window.orderBy("prev_city_min_region_length", "id")
w8 = Window.orderBy("city_min_region_length", "id").rowsBetween(-3, -1)
w9 = Window.partitionBy("region")

base = (
    df_stores
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-10-26")))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.length("region").alias("region_length"),
        F.coalesce("region", F.lit("OTHER")).alias("region_clean"),
    )
)

enriched = (
    base
    .withColumn("city_min_region_length", F.min("region_length").over(w))
    .withColumn("first_known_region", F.first("region", ignorenulls=True).over(w2))
)

deduped = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_windows = (
    deduped
    .select(
        "*",
        F.row_number().over(w4).alias("event_seq"),
        F.lag("city_min_region_length", 2, 0).over(w_part).alias("prev_city_min_region_length"),
        F.ntile(3).over(w6).alias("region_length_bucket"),
    )
)

eligible = (
    with_windows
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.quantity") > 222), "left_anti")
)

enriched_stores = (
    eligible
    .withColumn("prev_city_min_region_length_bucket", F.ntile(5).over(w7))
    .withColumn("city_min_region_length_avg_3_prior", F.avg("city_min_region_length").over(w8))
    .withColumn("region_sum_prev_city_min_region_length", F.sum("prev_city_min_region_length").over(w9))
)

result = (
    enriched_stores
    .rollup("city")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.round(F.sum("prev_city_min_region_length") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("prev_city_min_region_length_per_singapore"),
        F.sum(F.when(F.col("region") == "South", F.col("prev_city_min_region_length_bucket")).otherwise(0)).alias("south_prev_city_min_region_length_bucket"),
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("event_seq_per_london"),
        F.grouping("city").alias("grouping_id"),
    )
    .filter(F.col("south_prev_city_min_region_length_bucket") >= 0)
)
