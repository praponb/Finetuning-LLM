from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w_grp = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city", "region").orderBy("sqft", "id")
w5 = Window.partitionBy("region")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w7 = Window.orderBy(F.col("sqft").desc())
w8 = Window.orderBy("event_seq", "id")
w9 = Window.partitionBy("city").orderBy("running_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_stores
    .withColumn("running_sqft", F.sum("sqft").over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("last_known_region", F.last("region", ignorenulls=True).over(w_grp))
    .withColumn("prev_sqft", F.lag("sqft", 1).over(w4))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w5)), 4))
)

latest_match = (
    enriched
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

filtered = (
    latest_match
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-10-31"))) & F.col("region").isNotNull())
    .select("*", F.last_day("opened_date").alias("opened_date_month_end"))
)

scored = (
    filtered
    .withColumn("sqft_percent_rank", F.percent_rank().over(w7))
    .withColumn("event_seq_2", F.row_number().over(w8))
    .withColumn("cumulative_sqft_share", F.sum("sqft_share").over(w9))
)

totals = (
    scored
    .groupBy(
        "city",
        F.when(F.col("sqft_share") >= 0.66, "PREMIUM").otherwise("STANDARD").alias("sqft_share_tier"),
    )
    .agg(
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("region") == "North", 1)), F.lit(0)), 2).alias("sqft_per_north"),
        F.sum(F.when(F.col("region") == "East", F.col("inventory_quantity")).otherwise(0)).alias("east_inventory_quantity"),
        F.size(F.collect_set("region")).alias("num_regions"),
    )
)

result = (
    totals
    .select(
        "city",
        "sqft_share_tier",
        "avg_event_seq",
        "distinct_ids",
        "sqft_per_north",
        "east_inventory_quantity",
        "num_regions",
        F.when(F.col("city").rlike("^(Berlin|New York)$"), "PRIORITY").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("city_zone"),
    )
    .orderBy(F.col("city").desc_nulls_first(), "num_regions")
)
