from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w4 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("region", "city").orderBy(F.col("running_count_sqft_bucket").desc(), "id")
w7 = Window.partitionBy("region").orderBy(F.col("running_count_sqft_bucket").desc())
w8 = Window.partitionBy("region", "city")
w9 = Window.partitionBy("region")

scored = (
    df_stores
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.first("region", ignorenulls=True).over(w).alias("first_known_region"),
        F.first("region", ignorenulls=True).over(w_hist).alias("first_known_region_2"),
        F.ntile(10).over(w_recent).alias("sqft_bucket"),
        F.first("region", ignorenulls=True).over(w_hist).alias("first_known_region_3"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)

scored_stores = (
    scored
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "first_known_region",
        "sqft_bucket",
        F.max("sqft_bucket").over(w4).alias("running_max_sqft_bucket"),
        F.avg("sqft_bucket").over(w4).alias("running_avg_sqft_bucket"),
        F.min("sqft_bucket").over(w4).alias("running_min_sqft_bucket"),
        F.count("sqft_bucket").over(w4).alias("running_count_sqft_bucket"),
        F.last("region", ignorenulls=True).over(w5).alias("last_known_region"),
    )
)

not_flagged = (
    scored_stores
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") <= F.col("s.opened_date")), "left_anti")
)

enriched = (
    not_flagged
    .filter(F.col("store_name").like("%Mall%"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft_bucket",
        "running_max_sqft_bucket",
        "running_avg_sqft_bucket",
        F.ntile(5).over(w6).alias("running_count_sqft_bucket_bucket"),
        F.cume_dist().over(w7).alias("running_count_sqft_bucket_cume_dist"),
        F.round((F.col("running_min_sqft_bucket") / F.sum("running_min_sqft_bucket").over(w8)), 4).alias("running_min_sqft_bucket_share"),
        F.min("running_min_sqft_bucket").over(w9).alias("region_min_running_min_sqft_bucket"),
    )
)

result = (
    enriched
    .groupBy("region")
    .agg(
        F.count("*").alias("row_count"),
        F.count_if(F.col("region") == "Central").alias("central_events"),
        F.count("*").alias("row_count_2"),
    )
    .orderBy("row_count_2")
)
