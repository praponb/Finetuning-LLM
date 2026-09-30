from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city")
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc(), "id")
w5 = Window.orderBy("first_known_sqft", "id")
w6 = Window.partitionBy("region").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("region").orderBy("running_sqft_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("region").orderBy("region_count_sqft_share", "id")
w9 = Window.partitionBy("region").orderBy(F.col("prev_sqft_share").desc(), "id")
w10 = Window.partitionBy("region").orderBy(F.col("first_known_sqft").desc())
w11 = Window.partitionBy("region").orderBy("first_known_sqft")
w12 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())

metrics = (
    df_stores
    .withColumn("region_count_sqft", F.count("sqft").over(w))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4))
    .withColumn("first_known_sqft", F.first("sqft", ignorenulls=True).over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
)

metrics_stores = (
    metrics
    .select(
        "id",
        "region",
        "opened_date",
        "region_count_sqft",
        "first_known_sqft",
        F.round((F.col("region_count_sqft") / F.sum("region_count_sqft").over(w)), 4).alias("region_count_sqft_share"),
        F.lag("sqft_share", 2).over(w5).alias("prev_sqft_share"),
        F.sum("sqft_share").over(w6).alias("running_sqft_share"),
    )
)

scored = (
    metrics_stores
    .select(
        "*",
        F.sum("first_known_sqft").over(w7).alias("running_first_known_sqft"),
        F.lead("running_sqft_share", 2, 0).over(w8).alias("next_running_sqft_share"),
        F.ntile(10).over(w9).alias("prev_sqft_share_bucket"),
        F.cume_dist().over(w10).alias("first_known_sqft_cume_dist"),
    )
)

latest = (
    scored
    .withColumn("rn", F.rank().over(w11))
    .filter(F.col("rn") == 1)
)

latest_match = (
    latest
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn_2", F.row_number().over(w12))
    .filter(F.col("rn_2") == 1)
    .select(
        "s.id",
        "s.opened_date",
        "s.region_count_sqft",
        "s.region_count_sqft_share",
        "s.running_sqft_share",
        "s.first_known_sqft_cume_dist",
        F.col("c.units"),
    )
)

kept = (
    latest_match
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.channel").isin(["partner", "store"])), "left_anti")
)

kept_stores = (
    kept
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.revenue") <= 3800), "left_anti")
)

result = (
    kept_stores
    .groupBy(
        F.when(F.col("units") >= 33, "LARGE").otherwise("SMALL").alias("units_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("running_sqft_share"), 2).alias("avg_running_sqft_share"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .filter(F.col("distinct_ids") < 1000)
)
