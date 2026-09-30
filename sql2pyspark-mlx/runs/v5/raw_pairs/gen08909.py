from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("stock_qty").desc())
w2 = Window.orderBy(F.col("stock_qty").asc_nulls_last())
w3 = Window.partitionBy("product_brand")

valid = (
    df_inventory
    .filter(F.col("quantity") < 201)
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)

with_dims = (
    valid
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "inner")
    .filter(F.col("b.brand") == "Stark")
    .select(
        "src.id",
        "src.product_id",
        "src.last_restock",
        F.col("b.stock_qty"),
        F.col("b.brand"),
    )
)

enriched = (
    with_dims
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.brand").alias("product_brand"))
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    ranked
    .withColumn("stock_qty_rank", F.rank().over(w2))
    .withColumn("product_brand_max_stock_qty", F.max("stock_qty").over(w3))
)

filtered = (
    metrics
    .filter(F.col("brand").isNotNull())
    .select(
        "*",
        F.when(F.col("stock_qty") >= 404, "High").when(F.col("stock_qty") >= 194, "Medium").otherwise("Low").alias("stock_qty_band"),
    )
)

grouped = (
    filtered
    .withColumn("product_brand_max_stock_qty_tier", F.when(F.col("product_brand_max_stock_qty") >= 339, "PREMIUM").otherwise("STANDARD"))
    .withColumn("stock_qty_rank_tier", F.when(F.col("stock_qty_rank") >= 8, "PREMIUM").otherwise("STANDARD"))
    .cube("product_id", "product_brand_max_stock_qty_tier", "stock_qty_rank_tier")
    .agg(
        F.round(F.avg("product_brand_max_stock_qty"), 2).alias("avg_product_brand_max_stock_qty"),
        F.max("stock_qty").alias("max_stock_qty"),
        F.round(F.avg("stock_qty_rank"), 2).alias("avg_stock_qty_rank"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    grouped
    .select(
        "product_id",
        "product_brand_max_stock_qty_tier",
        "avg_product_brand_max_stock_qty",
        "max_stock_qty",
        "avg_stock_qty_rank",
    )
)
