from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city", "region").orderBy("last_restock", "id")
w6 = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w8 = Window.partitionBy("region").orderBy(F.col("sqft").asc_nulls_first(), "id")
w9 = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_last(), "id")

filtered = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("store_name")).alias("store_name_title"),
        F.coalesce("region", F.lit("N/A")).alias("region_clean"),
    )
)

with_last = (
    filtered
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.order_date"))
)

with_latest = (
    with_last
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock"))
)

metrics = (
    with_latest
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4))
    .withColumn("cumulative_sqft", F.sum("sqft").over(w4))
    .withColumn("next_sqft", F.lead("sqft", 1).over(w5))
    .withColumn("row_seq", F.row_number().over(w6))
)

with_latest_stores = (
    metrics
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

latest = (
    with_latest_stores
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

latest_stores = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w9))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest_stores
    .groupBy(
        F.when(F.col("sqft_share") >= 0.67, "HIGH").otherwise("LOW").alias("sqft_share_tier"),
    )
    .agg(
        F.count_if(F.col("region") == "Central").alias("central_events"),
        F.sum(F.when(F.col("region") == "North", F.col("sqft")).otherwise(0)).alias("north_sqft"),
        F.count("*").alias("event_count"),
        F.round(F.avg("next_sqft"), 2).alias("avg_next_sqft"),
        F.sum(F.when(F.col("city") == "London", F.col("next_sqft")).otherwise(0)).alias("london_next_sqft"),
    )
    .filter(F.col("london_next_sqft") >= 0)
    .orderBy(F.col("north_sqft").desc_nulls_first())
)
