from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("stock_qty").desc_nulls_first())
w_part = Window.partitionBy("supplier_id").orderBy("cost", "id").rowsBetween(-3, -1)

valid = (
    df_products
    .filter(F.col("supplier_id").isin([4, 22, 26, 29]))
    .withColumn("brand_product_name_key", F.concat_ws("/", "brand", "product_name"))
    .withColumn("resolved_brand", F.coalesce("brand", F.lit("N/A")))
)

filtered = (
    valid
    .filter((F.col("launch_date") > F.lit("2023-04-09").cast("date")) & ~F.col("category").isin(["Books", "Sports", "Fashion"]))
    .withColumn("product_name_normalized", F.upper(F.trim("product_name")))
    .withColumn("category_group", F.when(F.col("category").isin(["Electronics", "Fashion"]), "Tier 1").otherwise("Other"))
)

eligible = (
    filtered
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.product_id") == 38), "left_anti")
)

with_windows = (
    eligible
    .select(
        "id",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "brand_product_name_key",
        "resolved_brand",
        F.dense_rank().over(w).alias("stock_qty_dense_rank"),
        F.avg("stock_qty").over(w_part).alias("stock_qty_avg_3_prior"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("stock_qty") >= 389, "PREMIUM").otherwise("STANDARD").alias("stock_qty_tier"),
    )
    .agg(
        F.max("stock_qty").alias("max_stock_qty"),
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_count"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.max(F.when(F.col("brand") == "Stark", F.col("cost"))).alias("stark_max_cost"),
        F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_rows"),
    )
    .orderBy(F.col("stock_qty_bit_or").desc())
)
