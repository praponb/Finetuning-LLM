from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("price", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id", "store_id").orderBy("cumulative_price", "id")
w5 = Window.partitionBy("product_id").orderBy(F.col("cumulative_price").asc_nulls_first())
w6 = Window.orderBy("price_avg_6", "id")
w7 = Window.partitionBy("product_id").orderBy("sale_date", "id")
w_part = Window.orderBy("next_price", "id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-08-10").cast("date")) & (F.col("revenue") > 2900))
    .select("*", F.ceil("units").alias("units_whole"))
)

with_dims = (
    filtered
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.product_id",
        "e.sale_date",
        F.col("p.stock_qty"),
        F.col("p.price"),
    )
)

metrics = (
    with_dims
    .withColumn("price_avg_6", F.avg("price").over(w))
    .withColumn("store_id_min_stock_qty", F.min("stock_qty").over(w2))
    .withColumn("cumulative_price", F.sum("price").over(w3))
)

with_windows = (
    metrics
    .select(
        "id",
        "sale_date",
        "price_avg_6",
        "store_id_min_stock_qty",
        F.lead("price", 2).over(w4).alias("next_price"),
        F.percent_rank().over(w5).alias("cumulative_price_percent_rank"),
        F.ntile(5).over(w6).alias("price_avg_6_bucket"),
        F.row_number().over(w7).alias("event_seq"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "store_id_min_stock_qty",
        "next_price",
        "price_avg_6_bucket",
        "event_seq",
        F.avg("cumulative_price_percent_rank").over(w_part).alias("last7_avg_cumulative_price_percent_rank"),
        F.sum("cumulative_price_percent_rank").over(w_part).alias("last7_sum_cumulative_price_percent_rank"),
        F.count("cumulative_price_percent_rank").over(w_part).alias("last7_count_cumulative_price_percent_rank"),
    )
)

result = (
    scored
    .cube(
        F.when(F.col("event_seq") >= 8, "HIGH").otherwise("LOW").alias("event_seq_tier"),
        F.when(F.col("last7_avg_cumulative_price_percent_rank") >= 0.8, "PREMIUM").otherwise("STANDARD").alias("last7_avg_cumulative_price_percent_rank_tier"),
    )
    .agg(
        F.percentile_approx("next_price", [0.25, 0.5, 0.75]).alias("next_price_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("last7_sum_cumulative_price_percent_rank", 0.95).alias("last7_sum_cumulative_price_percent_rank_p95"),
    )
)
