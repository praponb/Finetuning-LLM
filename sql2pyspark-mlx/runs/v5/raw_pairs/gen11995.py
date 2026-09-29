from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

filtered = (
    df_stores
    .filter(F.col("opened_date") > F.lit("2023-03-16").cast("date"))
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        F.lower("store_name").alias("store_name_lower"),
        F.coalesce("region", F.lit("OTHER")).alias("region_filled"),
    )
)

with_windows = (
    filtered
    .filter(F.col("sqft") >= 7938)
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w)), 4))
    .withColumn("region_count_sqft", F.count("sqft").over(w))
)

with_last = (
    with_windows
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.region",
        "s.opened_date",
        "s.sqft",
        "s.store_name_lower",
        "s.region_filled",
        F.col("c.sale_date"),
    )
)

kept = (
    with_last
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") >= F.col("s.opened_date")), "left_semi")
)

result = (
    kept
    .groupBy("region")
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.count_if(F.col("region") == "East").alias("east_events"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("region") == "Central", F.col("sqft")).otherwise(0)).alias("central_sqft"),
    )
    .orderBy(F.col("central_sqft").desc_nulls_first())
)
