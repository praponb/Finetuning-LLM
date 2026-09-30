from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sqft", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("region").orderBy("opened_date", "id")
w3 = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc(), "id")
w4 = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")

valid = (
    df_stores
    .filter(F.col("sqft") >= 11864)
    .withColumn("store_name_slug", F.regexp_replace("store_name", " ", "_"))
    .withColumn("region_clean", F.coalesce("region", F.lit("N/A")))
)

not_flagged = (
    valid
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") < F.col("s.opened_date")), "left_anti")
)

with_windows = (
    not_flagged
    .select(
        "*",
        F.avg("sqft").over(w).alias("sqft_avg_5_prior"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("event_seq"),
        F.ntile(5).over(w4).alias("sqft_bucket"),
        F.row_number().over(w2).alias("event_seq_2"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("event_seq") >= 8, "HIGH").otherwise("LOW").alias("event_seq_tier"),
    )
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.countDistinct("city").alias("distinct_cities_2"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("sqft")).otherwise(0)).alias("bangkok_sqft"),
        F.countDistinct("region").alias("distinct_regions"),
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events"),
    )
    .orderBy(F.col("bangkok_events").desc_nulls_last())
)
