from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("supplier_id")
w3 = Window.partitionBy("seq_no").orderBy(F.col("acme_count").desc())

cleaned = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-09-16").cast("date")) & (F.col("category") == "Grocery"))
    .select(
        "id",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.when(F.col("price") >= 920, "High").when(F.col("price") >= 540, "Medium").otherwise("Low").alias("price_level"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("resolved_brand"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.corr("stock_qty", "price").over(w2).alias("corr_stock_qty_price"),
    )
)

kept_keys = (
    with_windows
    .select("supplier_id")
    .subtract(
        df_products
        .filter(F.lower(F.trim("product_name")) == "noise-cancel headphones")
        .select("supplier_id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

grouped = (
    kept_rows
    .groupBy(
        "seq_no",
        F.when(F.col("price") >= 1160, "PREMIUM").otherwise("STANDARD").alias("price_tier"),
    )
    .agg(
        F.percentile_approx("stock_qty", [0.25, 0.5, 0.75]).alias("stock_qty_quartiles"),
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_count"),
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_count"),
    )
    .withColumn("acme_count_dense_rank", F.dense_rank().over(w3))
)

result = grouped.select("seq_no", "price_tier", "acme_count")
