from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("sqft", "id")
w3 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy(F.col("is_north").desc_nulls_first())

cleaned = (
    df_stores
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-09-17")))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.lower("store_name").alias("store_name_lower"),
    )
)

valid = (
    cleaned
    .filter(F.col("region").isNotNull() & (~F.col("region").eqNullSafe(F.lit("North"))))
    .select("*", F.upper(F.trim("store_name")).alias("store_name_normalized"))
)

eligible = (
    valid
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.quantity") > 171), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date").between("2023-12-26", "2025-07-01")), "left_anti")
)

enriched = (
    not_flagged
    .select(
        "*",
        F.sum("sqft").over(w).alias("running_sqft"),
        F.lead("sqft", 1).over(w2).alias("next_sqft"),
        F.first("region", ignorenulls=True).over(w3).alias("first_known_region"),
    )
)

cleaned_stores = (
    enriched
    .filter(F.col("store_name").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "store_name_normalized",
        "running_sqft",
        "next_sqft",
        "first_known_region",
        F.when(F.col("region") == "North", 1).otherwise(0).alias("is_north"),
    )
)

ranked = (
    cleaned_stores
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .cube("city")
    .agg(
        F.count_if(F.col("city") == "Tokyo").alias("tokyo_events"),
        F.count_if(F.col("city") == "London").alias("london_events"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("is_north"))).alias("berlin_total_is_north"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("running_sqft")).otherwise(0)).alias("berlin_running_sqft"),
        F.grouping_id().alias("grouping_level"),
    )
)
