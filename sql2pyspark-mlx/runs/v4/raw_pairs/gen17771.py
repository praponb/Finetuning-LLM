from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id")
w2 = Window.partitionBy("category").orderBy(F.col("price_per_stock_qty").asc_nulls_last())
w3 = Window.partitionBy("brand")

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.col("launch_date") < "2023-08-01"))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        (F.col("price") / F.nullif("stock_qty", F.lit(0))).alias("price_per_stock_qty"),
        F.concat_ws("-", "product_name", "brand").alias("product_name_brand_key"),
    )
)

kept_keys = (
    filtered
    .select("category", "supplier_id")
    .intersect(
        df_products
        .filter(F.col("supplier_id").isNull())
        .select("category", "supplier_id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.category") == F.col("k.category")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .select(
        "*",
        F.avg("price").over(w).alias("supplier_id_avg_price"),
        F.dense_rank().over(w2).alias("price_per_stock_qty_dense_rank"),
        F.avg("price_per_stock_qty").over(w3).alias("brand_avg_price_per_stock_qty"),
    )
)

cleaned = (
    metrics
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "*",
        (F.col("cost") - F.col("supplier_id_avg_price")).alias("cost_minus_supplier_id_avg_price"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_filled"),
    )
)

result = (
    cleaned
    .withColumn("quarter_start", F.date_trunc("QUARTER", "launch_date"))
    .groupingSets(
        [["quarter_start", "brand"], ["quarter_start"]],
        "quarter_start",
        "brand",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_count"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
        F.grouping("quarter_start").alias("grouping_level"),
    )
    .orderBy(F.col("home_count").desc())
)
