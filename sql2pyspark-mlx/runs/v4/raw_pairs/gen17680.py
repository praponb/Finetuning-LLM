from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city", "region").orderBy("opened_date", "id")
w4 = Window.partitionBy("region").orderBy("sqft_share", "id")
w5 = Window.partitionBy("city", "region").orderBy("first_known_sqft", "id")
w6 = Window.partitionBy("city").orderBy(F.col("singapore_count").desc())

base = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-10-02").cast("date")) & F.col("region").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("city")).alias("city_title"),
        F.upper("city").alias("city_upper"),
        F.coalesce("region", F.lit("OTHER")).alias("region_filled"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 4).alias("sqft_share"),
        F.first("sqft", ignorenulls=True).over(w2).alias("first_known_sqft"),
        F.min("sqft").over(w).alias("city_min_sqft"),
    )
)

enriched = (
    metrics
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("next_city_min_sqft", F.lead("city_min_sqft", 1, 0).over(w4))
    .withColumn("row_seq", F.row_number().over(w5))
)

eligible = (
    enriched
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.opened_date")), "left_semi")
)

eligible_stores = (
    eligible
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.product_id").isin([1, 5, 9])), "left_anti")
)

result = (
    eligible_stores
    .groupBy("city", "region")
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", F.col("sqft")).otherwise(0)).alias("singapore_sqft"),
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.count_if(F.col("region") == "West").alias("west_events"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
    )
    .withColumn("singapore_count_percent_rank", F.percent_rank().over(w6))
)
