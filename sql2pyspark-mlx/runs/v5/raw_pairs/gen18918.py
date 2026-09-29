from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "supplier_id").orderBy("launch_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("supplier_id", "brand").orderBy("price", "id")
w3 = Window.partitionBy("supplier_id")
w4 = Window.partitionBy("brand", "category").orderBy("price", "id")

filtered = (
    df_products
    .filter(F.col("brand").isNotNull())
    .select(
        "*",
        F.substring("brand", 1, 4).alias("brand_prefix"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_filled"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.min("stock_qty").over(w).alias("last7_min_stock_qty"),
        F.max("stock_qty").over(w).alias("last7_max_stock_qty"),
        F.avg("stock_qty").over(w).alias("last7_avg_stock_qty"),
        F.ntile(10).over(w2).alias("price_bucket"),
    )
)

enriched = (
    metrics
    .filter(F.array_contains("tags", "clearance"))
    .select(
        "*",
        F.sum("price").over(w3).alias("supplier_id_sum_price"),
        F.ntile(10).over(w4).alias("price_bucket_2"),
    )
)

kept_keys = (
    enriched
    .select("category")
    .intersect(
        df_products
        .filter(F.col("product_name").isNull())
        .select("category")
    )
)

verified = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

stats = (
    verified
    .withColumn("last7_avg_stock_qty_tier", F.when(F.col("last7_avg_stock_qty") >= 350, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["brand", "last7_avg_stock_qty_tier"], ["brand"]],
        "brand",
        "last7_avg_stock_qty_tier",
    )
    .agg(
        F.count(F.when(F.col("category") == "Sports", 1)).alias("sports_rows"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.size(F.collect_set("category")).alias("num_categories"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.grouping("brand").alias("grouping_id"),
    )
    .filter(F.col("sports_rows") > 1)
)

result = (
    stats
    .select(
        "brand",
        "last7_avg_stock_qty_tier",
        "sports_rows",
        "brand_list",
        "num_categories",
        "stock_qty_bit_or",
        "grouping_id",
        F.when(F.col("brand").rlike("^(Umbrella|Initech)$"), "EXTENDED").otherwise("PRIORITY").alias("brand_zone"),
    )
    .orderBy(F.col("brand_zone").desc())
)
