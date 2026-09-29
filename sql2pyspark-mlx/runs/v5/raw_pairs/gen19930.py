from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("running_min_quantity").desc())
w5 = Window.partitionBy("store_id").orderBy(F.col("running_max_quantity").desc(), "id")

latest = (
    df_inventory
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

joined = (
    latest
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.launch_date"))
)

metrics = (
    joined
    .withColumn("running_max_quantity", F.max("quantity").over(w2))
    .withColumn("running_min_quantity", F.min("quantity").over(w2))
    .withColumn("running_avg_quantity", F.avg("quantity").over(w2))
)

with_windows = (
    metrics
    .select(
        "id",
        "quantity",
        "running_max_quantity",
        "running_min_quantity",
        "running_avg_quantity",
        F.ntile(10).over(w3).alias("quantity_bucket"),
        F.cume_dist().over(w4).alias("running_min_quantity_cume_dist"),
        F.ntile(4).over(w5).alias("running_max_quantity_bucket"),
    )
)

valid = (
    with_windows
    .filter(F.col("running_min_quantity") <= 100)
    .select(
        "*",
        F.round((F.col("running_min_quantity") * 0.85), 2).alias("running_min_quantity_scaled"),
    )
)

totals = (
    valid
    .rollup(
        F.when(F.col("running_min_quantity") >= 202, "HIGH").otherwise("LOW").alias("running_min_quantity_tier"),
        F.when(F.col("quantity") >= 181, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.percentile_approx("running_avg_quantity", [0.25, 0.5, 0.75]).alias("running_avg_quantity_quartiles"),
        F.min("running_min_quantity_cume_dist").alias("min_running_min_quantity_cume_dist"),
    )
)

result = (
    totals
    .select(
        "running_min_quantity_tier",
        "quantity_tier",
        "running_avg_quantity_quartiles",
        "min_running_min_quantity_cume_dist",
    )
)
