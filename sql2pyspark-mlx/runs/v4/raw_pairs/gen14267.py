from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("stock_qty", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("brand")
w3 = Window.partitionBy("brand").orderBy("price", "id")

with_windows = (
    df_products
    .select(
        "*",
        F.avg("stock_qty").over(w).alias("stock_qty_avg_2_prior"),
        F.corr("stock_qty", "cost").over(w2).alias("corr_stock_qty_cost"),
        F.ntile(10).over(w3).alias("price_bucket"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("price_bucket") >= 7, "PREMIUM").otherwise("STANDARD").alias("price_bucket_tier"),
        "category",
        "supplier_id",
    )
    .agg(
        F.percentile_approx("price_bucket", [0.25, 0.5, 0.75]).alias("price_bucket_quartiles"),
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_count"),
        F.percentile_approx("price_bucket", [0.25, 0.5, 0.75]).alias("price_bucket_quartiles_2"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.round(F.avg("price_bucket"), 2).alias("avg_price_bucket"),
    )
)
