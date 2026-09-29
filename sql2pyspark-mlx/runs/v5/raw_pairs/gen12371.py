from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("price", "id").rowsBetween(-6, Window.currentRow)
w_recent = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("brand").orderBy(F.col("price").desc(), "id")
w4 = Window.orderBy(F.col("stock_qty_filled").desc(), "id")
w5 = Window.partitionBy("brand").orderBy(F.col("stock_qty_avg_2").asc_nulls_first())
w6 = Window.partitionBy("brand").orderBy(F.col("stock_qty_filled_bucket_minus_stock_qty_filled_avg_6").desc_nulls_last())

cleaned = (
    df_products
    .filter(F.col("stock_qty").isNotNull() & (F.col("launch_date") >= F.lit("2023-05-13").cast("date")))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "tags",
        "supplier_id",
        F.coalesce("stock_qty", F.lit(0)).alias("stock_qty_filled"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_clean"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "product_name",
        "brand",
        "stock_qty",
        "tags",
        "brand_clean",
        F.avg("stock_qty_filled").over(w).alias("stock_qty_filled_avg_6"),
        F.avg("stock_qty").over(w_recent).alias("stock_qty_avg_2"),
        F.row_number().over(w3).alias("row_seq"),
        F.ntile(5).over(w4).alias("stock_qty_filled_bucket"),
    )
)

matched_keys = (
    metrics
    .select("id", "brand")
    .intersectAll(
        df_products
        .filter(F.col("stock_qty") != 112)
        .select("id", "brand")
    )
)

verified = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("brand").isNotNull() & F.col("stock_qty").between(304, 388))
    .select(
        "id",
        "product_name",
        "brand",
        "tags",
        "brand_clean",
        "stock_qty_filled_avg_6",
        "stock_qty_avg_2",
        "stock_qty_filled_bucket",
        F.initcap(F.trim("brand")).alias("brand_title"),
        F.upper("brand").alias("brand_upper"),
        F.coalesce("brand", F.lit("N/A")).alias("resolved_brand"),
    )
)

cleaned_products = (
    valid
    .filter(F.col("brand").isNotNull() & (F.col("stock_qty_filled_bucket") != 3))
    .withColumn("stock_qty_filled_bucket_minus_stock_qty_filled_avg_6", (F.col("stock_qty_filled_bucket") - F.col("stock_qty_filled_avg_6")))
)

scored = (
    cleaned_products
    .filter(F.col("stock_qty_filled_bucket_minus_stock_qty_filled_avg_6").between(21, 52))
    .select(
        "id",
        "product_name",
        "brand",
        "tags",
        "brand_clean",
        "brand_title",
        "resolved_brand",
        F.percent_rank().over(w5).alias("stock_qty_avg_2_percent_rank"),
        F.dense_rank().over(w6).alias("stock_qty_filled_bucket_minus_stock_qty_filled_avg_6_dense_rank"),
    )
)

eligible = (
    scored
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.order_id").isin([14, 18, 38])), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "product_name",
        "tags",
        "brand_clean",
        "brand_title",
        "resolved_brand",
        "stock_qty_avg_2_percent_rank",
        F.when(F.col("product_name").rlike("^(USB-C Hub|Smart Watch)$"), "PRIORITY").otherwise("GROUP_A").alias("product_name_zone"),
    )
    .orderBy(F.col("stock_qty_avg_2_percent_rank").desc_nulls_last())
)
