from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("product_id").orderBy("adjusted_quantity", "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("adjusted_quantity").asc_nulls_last(), "id")
w_ordered = Window.orderBy("adjusted_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("brand", "product_id").orderBy("cumulative_adjusted_quantity_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_inventory
    .filter(F.col("quantity").isNotNull())
    .select(
        "id",
        "product_id",
        "last_restock",
        F.round((F.col("quantity") * 1.07), 2).alias("adjusted_quantity"),
    )
)

metrics = (
    base
    .filter(F.month("last_restock").isin([4, 10, 11]))
    .withColumn("product_id_stddev_samp_adjusted_quantity", F.stddev_samp("adjusted_quantity").over(w))
    .withColumn("cumulative_adjusted_quantity", F.sum("adjusted_quantity").over(w2))
    .withColumn("adjusted_quantity_bucket", F.ntile(4).over(w_part))
)

base_inventory = (
    metrics
    .filter(F.col("last_restock") < "2025-09-01")
    .select(
        "*",
        F.ceil("cumulative_adjusted_quantity").alias("cumulative_adjusted_quantity_whole"),
    )
)

ranked = (
    base_inventory
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    ranked
    .select(
        "*",
        F.sum("adjusted_quantity_bucket").over(w_ordered).alias("running_adjusted_quantity_bucket"),
        F.last("product_id_stddev_samp_adjusted_quantity", ignorenulls=True).over(w_ordered).alias("last_known_product_id_stddev_samp_adjusted_quantity"),
    )
)

with_dims = (
    with_windows
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .select(
        "e.id",
        "e.product_id",
        "e.last_restock",
        "e.cumulative_adjusted_quantity",
        "e.cumulative_adjusted_quantity_whole",
        "e.row_num",
        "e.running_adjusted_quantity_bucket",
        "e.last_known_product_id_stddev_samp_adjusted_quantity",
        F.col("p.cost"),
        F.col("p.brand"),
    )
)

metrics_inventory = (
    with_dims
    .select(
        "id",
        "last_restock",
        "cumulative_adjusted_quantity",
        "cumulative_adjusted_quantity_whole",
        "row_num",
        "brand",
        F.max("cumulative_adjusted_quantity_whole").over(w_running).alias("running_max_cumulative_adjusted_quantity_whole"),
        F.min("cumulative_adjusted_quantity_whole").over(w_running).alias("running_min_cumulative_adjusted_quantity_whole"),
        F.avg("cumulative_adjusted_quantity_whole").over(w_running).alias("running_avg_cumulative_adjusted_quantity_whole"),
        F.count("cumulative_adjusted_quantity_whole").over(w_running).alias("running_count_cumulative_adjusted_quantity_whole"),
        F.last("cumulative_adjusted_quantity", ignorenulls=True).over(w7).alias("last_known_cumulative_adjusted_quantity"),
    )
)

stats = (
    metrics_inventory
    .groupBy(
        F.date_trunc("QUARTER", "last_restock").alias("quarter_start"),
        F.when(F.col("running_max_cumulative_adjusted_quantity_whole") >= 74, "PREMIUM").otherwise("STANDARD").alias("running_max_cumulative_adjusted_quantity_whole_tier"),
    )
    .agg(
        F.percentile_approx("cumulative_adjusted_quantity", 0.9).alias("cumulative_adjusted_quantity_p90"),
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_count"),
        F.round(F.sum("last_known_cumulative_adjusted_quantity") / F.nullif(F.count(F.when(F.col("brand") == "Stark", 1)), F.lit(0)), 2).alias("last_known_cumulative_adjusted_quantity_per_stark"),
        F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_rows"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.max(F.when(F.col("brand") == "Umbrella", F.col("running_avg_cumulative_adjusted_quantity_whole"))).alias("umbrella_max_running_avg_cumulative_adjusted_quantity_whole"),
    )
    .filter(F.col("last_known_cumulative_adjusted_quantity_per_stark") > 0)
)

result = (
    stats
    .select(
        "quarter_start",
        "running_max_cumulative_adjusted_quantity_whole_tier",
        "cumulative_adjusted_quantity_p90",
        "stark_count",
        "last_known_cumulative_adjusted_quantity_per_stark",
        "initech_rows",
        "num_brands",
    )
)
