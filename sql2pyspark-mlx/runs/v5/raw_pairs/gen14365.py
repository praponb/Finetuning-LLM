from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("revenue", "id")
w4 = Window.orderBy(F.col("revenue").desc(), "id")
w5 = Window.partitionBy("product_id").orderBy("sale_date", "id")
w6 = Window.orderBy("revenue_bucket", "id").rowsBetween(-5, -1)

deduped = (
    df_store_sales
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    deduped
    .select(
        "*",
        F.max("revenue").over(w2).alias("running_max_revenue"),
        F.count("revenue").over(w2).alias("running_count_revenue"),
        F.avg("revenue").over(w2).alias("running_avg_revenue"),
        F.min("revenue").over(w2).alias("running_min_revenue"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.ntile(4).over(w3).alias("revenue_bucket"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

with_windows = (
    enriched
    .filter(F.col("revenue_bucket").between(4, 7))
    .select(
        "id",
        "sale_date",
        "units",
        "revenue",
        "latest_rank",
        "running_max_revenue",
        "revenue_bucket",
        "event_seq",
        F.lead("running_max_revenue").over(w5).alias("next_running_max_revenue"),
        F.lag("running_max_revenue").over(w5).alias("prev_running_max_revenue"),
        F.lead("units").over(w5).alias("next_units"),
        F.lag("units").over(w5).alias("prev_units"),
        F.row_number().over(w5).alias("seq_no"),
        F.avg("running_count_revenue").over(w6).alias("running_count_revenue_avg_5_prior"),
    )
)

filtered = (
    with_windows
    .filter(F.col("sale_date") > F.lit("2023-06-17").cast("date"))
    .withColumn("seq_no_whole", F.floor("seq_no"))
    .withColumn("event_seq_times_prev_running_max_revenue", (F.col("event_seq") * F.col("prev_running_max_revenue")))
)

result = (
    filtered
    .withColumn("year_start", F.date_trunc("YEAR", "sale_date"))
    .withColumn("next_units_tier", F.when(F.col("next_units") >= 43, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["year_start", "next_units_tier"], ["year_start"], []],
        "year_start",
        "next_units_tier",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("prev_running_max_revenue"), 2).alias("avg_prev_running_max_revenue"),
        F.bit_or("units").alias("units_bit_or"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.min("revenue_bucket").alias("min_revenue_bucket"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy("distinct_ids_2")
)
