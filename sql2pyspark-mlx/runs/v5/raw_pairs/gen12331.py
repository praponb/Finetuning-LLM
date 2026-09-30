from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")

valid = (
    df_stores
    .filter((F.col("sqft") > 11311) & (F.col("opened_date") > F.lit("2023-09-24").cast("date")))
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        F.initcap(F.trim("region")).alias("region_title"),
        F.coalesce("region", F.lit("OTHER")).alias("region_filled"),
    )
)

metrics = (
    valid
    .filter(F.col("store_name").like("%Kiosk%"))
    .select(
        "*",
        F.max("sqft").over(w_hist).alias("last7_max_sqft"),
        F.min("sqft").over(w_hist).alias("last7_min_sqft"),
        F.count("sqft").over(w_hist).alias("last7_count_sqft"),
        F.sum("sqft").over(w_hist).alias("last7_sum_sqft"),
    )
)

kept = (
    metrics
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") < F.col("s.opened_date")), "left_semi")
)

ranked = (
    kept
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

totals = (
    ranked
    .groupBy(
        F.when(F.col("last7_count_sqft") >= 17, "HIGH").otherwise("LOW").alias("last7_count_sqft_tier"),
    )
    .agg(
        F.count_if(F.col("city") == "Chiang Mai").alias("chiang_mai_events"),
        F.sum(F.when(F.col("city") == "New York", F.col("last7_count_sqft")).otherwise(0)).alias("new_york_last7_count_sqft"),
    )
    .filter(F.col("new_york_last7_count_sqft") >= 0)
)

result = (
    totals
    .select("last7_count_sqft_tier", "chiang_mai_events", "new_york_last7_count_sqft")
    .orderBy(F.col("new_york_last7_count_sqft").desc())
)
