from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("last_restock", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy(F.col("last4_min_product_name_length").desc())
w3 = Window.partitionBy("store_id").orderBy("last4_count_product_name_length", "id")
w4 = Window.partitionBy("category").orderBy(F.col("stock_qty").desc(), "id")
w5 = Window.partitionBy("store_id").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy("last4_count_product_name_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("store_id").orderBy(F.col("last4_sum_product_name_length").asc_nulls_last())
w8 = Window.orderBy("prev_stock_qty", "id")
w_part = Window.partitionBy("store_id").orderBy("first_known_prev_stock_qty", "id").rowsBetween(-3, Window.currentRow)
w10 = Window.partitionBy("store_id").orderBy("last4_sum_product_name_length_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_inventory
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .filter(F.col("d.category").isin(["Electronics", "Grocery"]))
    .select("src.*", F.col("d.category"), F.col("d.stock_qty"), F.col("d.product_name"))
)

base = (
    enriched
    .filter(F.col("last_restock") >= F.lit("2023-06-21").cast("date"))
    .select(
        "*",
        F.length("product_name").alias("product_name_length"),
        F.length("product_name").alias("product_name_length_2"),
        F.coalesce("category", F.lit("DIRECT")).alias("resolved_category"),
    )
)

scored = (
    base
    .select(
        "id",
        "store_id",
        "category",
        "stock_qty",
        "product_name_length",
        F.min("product_name_length").over(w).alias("last4_min_product_name_length"),
        F.sum("product_name_length").over(w).alias("last4_sum_product_name_length"),
        F.count("product_name_length").over(w).alias("last4_count_product_name_length"),
    )
)

latest = (
    scored
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    latest
    .withColumn("prev_stock_qty", F.lag("stock_qty", 2, 0).over(w3))
    .withColumn("stock_qty_bucket", F.ntile(3).over(w4))
)

joined = (
    metrics
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "inner")
    .select(
        "s.id",
        "s.store_id",
        "s.product_name_length",
        "s.last4_sum_product_name_length",
        "s.last4_count_product_name_length",
        "s.latest_rank",
        "s.prev_stock_qty",
        F.col("b.store_name"),
        F.col("b.opened_date"),
    )
)

scored_inventory = (
    joined
    .withColumn("cumulative_last4_count_product_name_length", F.sum("last4_count_product_name_length").over(w5))
    .withColumn("running_last4_count_product_name_length", F.sum("last4_count_product_name_length").over(w6))
    .withColumn("last4_sum_product_name_length_percent_rank", F.percent_rank().over(w7))
    .withColumn("prev_stock_qty_bucket", F.ntile(5).over(w8))
    .withColumn("first_known_prev_stock_qty", F.first("prev_stock_qty", ignorenulls=True).over(w6))
)

result = (
    scored_inventory
    .select(
        "*",
        F.max("prev_stock_qty").over(w_part).alias("last4_max_prev_stock_qty"),
        F.avg("prev_stock_qty").over(w_part).alias("last4_avg_prev_stock_qty"),
        F.sum("prev_stock_qty").over(w_part).alias("last4_sum_prev_stock_qty"),
        F.sum("first_known_prev_stock_qty").over(w10).alias("running_first_known_prev_stock_qty"),
    )
)
