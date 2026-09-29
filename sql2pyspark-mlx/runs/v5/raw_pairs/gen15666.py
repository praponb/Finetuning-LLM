from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")

enriched = (
    df_order_items
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .filter(F.col("d.supplier_id").isNotNull())
    .select(
        "src.id",
        "src.order_id",
        "src.product_id",
        "src.unit_price",
        F.col("d.product_name"),
        F.col("d.brand"),
        F.col("d.cost"),
    )
)

joined = (
    enriched
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.product_id",
        "a.unit_price",
        "a.brand",
        F.col("b.product_name").alias("product_product_name"),
        F.col("b.brand").alias("product_brand"),
    )
)

ranked = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "product_id", "unit_price", "brand")
)

cleaned = (
    ranked
    .filter(F.col("product_id").isNotNull())
    .withColumn("brand_title", F.initcap(F.trim("brand")))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("unit_price") >= 410, "LARGE").otherwise("SMALL").alias("unit_price_tier"),
    )
    .agg(
        F.percentile_approx("unit_price", 0.9).alias("unit_price_p90"),
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_count"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
    .filter(F.col("avg_unit_price") >= 0)
    .orderBy(F.col("avg_unit_price").desc_nulls_first())
)
