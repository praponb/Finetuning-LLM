from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("cost").asc_nulls_first(), "id")
w2 = Window.orderBy(F.col("cost").desc_nulls_first())
w3 = Window.orderBy("stock_qty", "id")
w4 = Window.orderBy(F.col("cost").desc(), "id")
w5 = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w_part = Window.orderBy("running_min_stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_products
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "product_name", "cost", "stock_qty", "launch_date", "tags")
)

with_windows = (
    deduped
    .filter(F.col("launch_date") >= "2024-11-01")
    .select(
        "*",
        F.percent_rank().over(w2).alias("cost_percent_rank"),
        F.lead("cost", 2).over(w3).alias("next_cost"),
        F.ntile(10).over(w4).alias("cost_bucket"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "cost",
        "tags",
        F.avg("stock_qty").over(w5).alias("running_avg_stock_qty"),
        F.sum("stock_qty").over(w5).alias("running_sum_stock_qty"),
        F.count("stock_qty").over(w5).alias("running_count_stock_qty"),
        F.min("stock_qty").over(w5).alias("running_min_stock_qty"),
    )
)

with_latest = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date").alias("store_sale_sale_date"))
)

metrics_products = (
    with_latest
    .select(
        "id",
        "running_avg_stock_qty",
        "running_sum_stock_qty",
        "running_count_stock_qty",
        F.sum("running_avg_stock_qty").over(w_part).alias("running_sum_running_avg_stock_qty"),
        F.count("running_avg_stock_qty").over(w_part).alias("running_count_running_avg_stock_qty"),
        F.max("running_avg_stock_qty").over(w_part).alias("running_max_running_avg_stock_qty"),
    )
)

summary = (
    metrics_products
    .groupBy(
        F.when(F.col("running_count_stock_qty") >= 13, "LARGE").otherwise("SMALL").alias("running_count_stock_qty_tier"),
    )
    .agg(
        F.avg("running_count_stock_qty").alias("avg_running_count_stock_qty"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.round(F.avg("running_sum_running_avg_stock_qty"), 2).alias("avg_running_sum_running_avg_stock_qty"),
        F.percentile_approx("running_avg_stock_qty", 0.9).alias("running_avg_stock_qty_p90"),
    )
)

result = (
    summary
    .select(
        "running_count_stock_qty_tier",
        "avg_running_count_stock_qty",
        "distinct_ids",
        "row_count",
        "avg_running_sum_running_avg_stock_qty",
    )
    .orderBy(F.col("avg_running_count_stock_qty").desc())
)
