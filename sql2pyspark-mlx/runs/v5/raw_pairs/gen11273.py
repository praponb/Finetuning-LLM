from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("opened_date", "id")
w3 = Window.partitionBy("city").orderBy("first_known_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy(F.col("first_known_sqft").asc_nulls_last())
w5 = Window.orderBy(F.col("sqft").desc_nulls_first())
w6 = Window.partitionBy("region").orderBy("sqft", "id")
w7 = Window.partitionBy("city").orderBy("row_seq", "id")
w_hist = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("region", "city").orderBy("sqft_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("city").orderBy(F.col("sqft_bucket").desc(), "id")
w_hist_2 = Window.orderBy("sqft_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

enriched = (
    df_stores
    .filter(F.col("store_name").isNotNull())
    .withColumn("first_known_sqft", F.first("sqft", ignorenulls=True).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
)

enriched_stores = (
    enriched
    .select(
        "id",
        "city",
        "region",
        "sqft",
        "row_seq",
        F.first("sqft", ignorenulls=True).over(w3).alias("first_known_sqft_2"),
        F.cume_dist().over(w4).alias("first_known_sqft_cume_dist"),
    )
)

with_windows = (
    enriched_stores
    .withColumn("sqft_cume_dist", F.cume_dist().over(w5))
    .withColumn("sqft_bucket", F.ntile(10).over(w6))
    .withColumn("row_seq_bucket", F.ntile(10).over(w7))
)

metrics = (
    with_windows
    .select(
        "id",
        "region",
        "row_seq",
        "first_known_sqft_2",
        "sqft_cume_dist",
        "row_seq_bucket",
        F.sum("sqft").over(w_hist).alias("running_sqft"),
        F.last("first_known_sqft_2", ignorenulls=True).over(w9).alias("last_known_first_known_sqft_2"),
        F.ntile(5).over(w10).alias("sqft_bucket_bucket"),
        F.sum("sqft_bucket").over(w_hist_2).alias("cumulative_sqft_bucket"),
    )
)

kept = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.year(F.col("ch.sale_date")) == 2025), "left_anti")
)

with_latest = (
    kept
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w12))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.revenue").alias("store_sale_revenue"),
        F.col("c.sale_date").alias("store_sale_sale_date"),
    )
)

stats = (
    with_latest
    .withColumn("activity_month", F.date_trunc("MONTH", "store_sale_sale_date"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "store_sale_sale_date"))
    .groupingSets(
        [["activity_month", "quarter_start", "region"], ["activity_month", "quarter_start"], ["activity_month"]],
        "activity_month",
        "quarter_start",
        "region",
    )
    .agg(
        F.max(F.when(F.col("region") == "East", F.col("row_seq"))).alias("east_max_row_seq"),
        F.count(F.when(F.col("region") == "East", 1)).alias("east_count"),
        F.count_if(F.col("region") == "North").alias("north_events"),
        F.sum(F.when(F.col("region") == "Central", F.col("row_seq")).otherwise(0)).alias("central_row_seq"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("north_events") > 1)
)

result = stats.select("activity_month", "quarter_start", "region", "central_row_seq")
