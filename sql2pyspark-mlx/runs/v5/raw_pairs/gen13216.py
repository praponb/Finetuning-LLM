from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("opened_date", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w4 = Window.orderBy("store_sale_sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("region").orderBy("last7_count_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("region").orderBy("last7_count_sqft", "id")
w7 = Window.partitionBy("region")
w8 = Window.partitionBy("region").orderBy("store_sale_sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("region").orderBy(F.col("next_last7_max_sqft").desc_nulls_first(), "id")

enriched = (
    df_stores
    .select(
        "id",
        "store_name",
        "region",
        F.max("sqft").over(w_ordered).alias("last7_max_sqft"),
        F.avg("sqft").over(w_ordered).alias("last7_avg_sqft"),
        F.min("sqft").over(w_ordered).alias("last7_min_sqft"),
        F.count("sqft").over(w_ordered).alias("last7_count_sqft"),
        F.lag("sqft", 1, 0).over(w2).alias("prev_sqft"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.units").alias("store_sale_units"),
        F.col("c.sale_date").alias("store_sale_sale_date"),
    )
)

kept = (
    with_latest
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.store_id") == 20), "left_semi")
)

enriched_stores = (
    kept
    .select(
        "id",
        "store_name",
        "region",
        "last7_max_sqft",
        "last7_avg_sqft",
        "last7_count_sqft",
        F.sum("last7_max_sqft").over(w4).alias("cumulative_last7_max_sqft"),
        F.last("store_sale_units", ignorenulls=True).over(w5).alias("last_known_store_sale_units"),
        F.lead("last7_max_sqft", 1).over(w6).alias("next_last7_max_sqft"),
        F.round((F.col("last7_avg_sqft") / F.sum("last7_avg_sqft").over(w7)), 4).alias("last7_avg_sqft_share"),
        F.last("last7_avg_sqft", ignorenulls=True).over(w8).alias("last_known_last7_avg_sqft"),
    )
)

ranked = (
    enriched_stores
    .withColumn("dedup_rank", F.row_number().over(w9))
    .filter(F.col("dedup_rank") == 1)
)

stats = (
    ranked
    .groupBy(
        F.when(F.col("last7_avg_sqft_share") >= 0.69, "HIGH").otherwise("LOW").alias("last7_avg_sqft_share_tier"),
        "region",
    )
    .agg(
        F.count(F.when(F.col("region") == "South", 1)).alias("south_count"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.avg("last7_avg_sqft").alias("avg_last7_avg_sqft"),
        F.count(F.when(F.col("region") == "East", 1)).alias("east_count"),
        F.percentile_approx("next_last7_max_sqft", 0.5).alias("next_last7_max_sqft_p50"),
        F.min("last7_count_sqft").alias("min_last7_count_sqft"),
    )
)

result = (
    stats
    .select(
        "last7_avg_sqft_share_tier",
        "region",
        "region_list",
        "avg_last7_avg_sqft",
        "east_count",
    )
    .orderBy(F.col("region").asc_nulls_last())
)
