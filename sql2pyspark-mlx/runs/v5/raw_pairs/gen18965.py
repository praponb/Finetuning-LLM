from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date_plus_84d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id").orderBy("product_id_min_units")
w4 = Window.orderBy("product_id_min_units", "id")
w_hist = Window.orderBy(F.col("product_id_min_units").desc_nulls_last())
w6 = Window.orderBy(F.col("product_id_min_units").desc(), "id")
w7 = Window.orderBy("sale_date_plus_84d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("product_id_min_units_bucket", "id")
w9 = Window.orderBy("product_id_min_units_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("next_product_id_min_units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("last_known_product_id_min_units_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("first_known_product_id_min_units", "id")
w13 = Window.orderBy("product_id_min_units_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w14 = Window.partitionBy("year_start").orderBy(F.col("avg_running_avg_first_known_product_id_min_units").desc())

base = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-10-03").cast("date"))
    .select(
        "id",
        "product_id",
        "units",
        F.date_add("sale_date", 84).alias("sale_date_plus_84d"),
    )
)

enriched = (
    base
    .filter(F.col("units").between(12, 13))
    .select(
        "id",
        "product_id",
        "units",
        "sale_date_plus_84d",
        F.first("units", ignorenulls=True).over(w).alias("first_known_units"),
        F.min("units").over(w2).alias("product_id_min_units"),
    )
)

latest = (
    enriched
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
    .select("id", "sale_date_plus_84d", "product_id_min_units")
)

scored = (
    latest
    .withColumn("product_id_min_units_bucket", F.ntile(10).over(w4))
    .withColumn("product_id_min_units_dense_rank", F.dense_rank().over(w_hist))
    .withColumn("product_id_min_units_bucket_2", F.ntile(10).over(w6))
    .withColumn("first_known_product_id_min_units", F.first("product_id_min_units", ignorenulls=True).over(w7))
)

enriched_store_sales = (
    scored
    .select(
        "id",
        "sale_date_plus_84d",
        "product_id_min_units",
        "product_id_min_units_dense_rank",
        "first_known_product_id_min_units",
        F.lead("product_id_min_units", 1).over(w_part).alias("next_product_id_min_units"),
        F.last("product_id_min_units_bucket", ignorenulls=True).over(w9).alias("last_known_product_id_min_units_bucket"),
        F.lag("first_known_product_id_min_units", 1, 0).over(w_part).alias("prev_first_known_product_id_min_units"),
    )
)

metrics = (
    enriched_store_sales
    .select(
        "*",
        F.max("first_known_product_id_min_units").over(w10).alias("running_max_first_known_product_id_min_units"),
        F.count("first_known_product_id_min_units").over(w10).alias("running_count_first_known_product_id_min_units"),
        F.avg("first_known_product_id_min_units").over(w10).alias("running_avg_first_known_product_id_min_units"),
        F.first("product_id_min_units", ignorenulls=True).over(w11).alias("first_known_product_id_min_units_2"),
    )
)

metrics_store_sales = (
    metrics
    .withColumn("next_product_id_min_units_dense_rank", F.lead("product_id_min_units_dense_rank").over(w_ordered))
    .withColumn("prev_product_id_min_units_dense_rank", F.lag("product_id_min_units_dense_rank").over(w_ordered))
    .withColumn("prev_product_id_min_units", F.lag("product_id_min_units").over(w_ordered))
    .withColumn("next_product_id_min_units_2", F.lead("product_id_min_units").over(w_ordered))
    .withColumn("event_seq", F.row_number().over(w_ordered))
    .withColumn("last_known_product_id_min_units", F.last("product_id_min_units", ignorenulls=True).over(w13))
)

result = (
    metrics_store_sales
    .groupBy(
        F.date_trunc("YEAR", "sale_date_plus_84d").alias("year_start"),
        F.date_trunc("QUARTER", "sale_date_plus_84d").alias("quarter_start"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("event_seq", 0.9).alias("event_seq_p90"),
        F.avg("running_avg_first_known_product_id_min_units").alias("avg_running_avg_first_known_product_id_min_units"),
        F.percentile_approx("next_product_id_min_units_dense_rank", [0.25, 0.5, 0.75]).alias("next_product_id_min_units_dense_rank_quartiles"),
        F.percentile_approx("next_product_id_min_units", 0.5).alias("next_product_id_min_units_p50"),
    )
    .withColumn("avg_running_avg_first_known_product_id_min_units_dense_rank", F.dense_rank().over(w14))
    .orderBy("quarter_start")
)
