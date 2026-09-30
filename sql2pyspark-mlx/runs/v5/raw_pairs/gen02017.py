from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region", "city").orderBy("opened_date_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("city").orderBy(F.col("opened_date_month").desc(), "id")
w5 = Window.partitionBy("city").orderBy(F.col("row_seq").desc_nulls_last())
w6 = Window.partitionBy("city")
w7 = Window.partitionBy("city").orderBy(F.col("row_seq").desc(), "id")
w8 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("city").orderBy("last_known_opened_date_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stores
    .filter(F.col("opened_date") > F.lit("2023-03-04").cast("date"))
    .select("*", F.month("opened_date").alias("opened_date_month"))
)

eligible = (
    cleaned
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") <= F.col("s.opened_date")), "left_anti")
)

scored = (
    eligible
    .filter(F.col("sqft") == 10833)
    .select(
        "*",
        F.last("opened_date_month", ignorenulls=True).over(w).alias("last_known_opened_date_month"),
        F.sum("opened_date_month").over(w2).alias("cumulative_opened_date_month"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.avg("cumulative_opened_date_month").over(w3).alias("cumulative_opened_date_month_avg_6_prior"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.cume_dist().over(w5).alias("row_seq_cume_dist"),
        F.stddev_samp("cumulative_opened_date_month").over(w6).alias("city_stddev_samp_cumulative_opened_date_month"),
        F.ntile(4).over(w7).alias("row_seq_bucket"),
        F.last("opened_date_month", ignorenulls=True).over(w8).alias("last_known_opened_date_month_2"),
        F.sum("cumulative_opened_date_month_avg_6_prior").over(w9).alias("running_cumulative_opened_date_month_avg_6_prior"),
    )
)

cleaned_stores = (
    metrics
    .filter(F.col("city_stddev_samp_cumulative_opened_date_month").between(212, 262))
    .withColumn("is_west", F.when(F.col("region") == "West", 1).otherwise(0))
    .withColumn("resolved_region", F.coalesce("region", F.lit("DIRECT")))
)

result = (
    cleaned_stores
    .groupBy("city")
    .pivot("region", ["East", "South"])
    .agg(F.avg("row_seq_bucket"))
)
