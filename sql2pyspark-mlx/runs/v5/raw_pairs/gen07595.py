from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("category").orderBy("cost", "id").rowsBetween(-2, Window.currentRow)

with_windows = (
    df_products
    .filter(F.array_contains("tags", "premium"))
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.count("stock_qty").over(w_running).alias("last3_count_stock_qty"),
        F.avg("stock_qty").over(w_running).alias("last3_avg_stock_qty"),
        F.sum("stock_qty").over(w_running).alias("last3_sum_stock_qty"),
    )
)

result = (
    with_windows
    .withColumn("last3_avg_stock_qty_tier", F.when(F.col("last3_avg_stock_qty") >= 310, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["brand", "last3_avg_stock_qty_tier"], ["brand"], []],
        "brand",
        "last3_avg_stock_qty_tier",
    )
    .agg(
        F.corr("cost", "price").alias("corr_cost_price"),
        F.percentile_approx("last3_avg_stock_qty", 0.9).alias("last3_avg_stock_qty_p90"),
        F.percentile_approx("last3_count_stock_qty", [0.25, 0.5, 0.75]).alias("last3_count_stock_qty_quartiles"),
        F.round(F.avg("last3_count_stock_qty"), 2).alias("avg_last3_count_stock_qty"),
        F.max(F.when(F.col("brand") == "Umbrella", F.col("last3_avg_stock_qty"))).alias("umbrella_max_last3_avg_stock_qty"),
    )
    .filter(F.col("last3_avg_stock_qty_p90") > 0)
    .orderBy(F.col("umbrella_max_last3_avg_stock_qty").desc())
)
