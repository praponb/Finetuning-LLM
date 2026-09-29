from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("category").orderBy(F.col("product_stock_qty").desc_nulls_last(), "id")

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.trim("city").alias("clean_city"),
        F.when(F.col("city").isin(["Berlin", "Singapore"]), "Priority").otherwise("Other").alias("city_group"),
    )
)

filtered = (
    base
    .filter(F.col("city").eqNullSafe(F.lit("Chiang Mai")))
    .withColumn("clean_country", F.trim("country"))
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "city", "clean_country")
)

with_latest = (
    ranked
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.clean_country",
        F.col("c.stock_qty").alias("product_stock_qty"),
        F.col("c.category"),
    )
)

eligible = (
    with_latest
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.size(F.col("ch.tags")) > 1), "left_anti")
)

deduped = (
    eligible
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("product_stock_qty") >= 318, "PREMIUM").otherwise("STANDARD").alias("product_stock_qty_tier"),
        "category",
    )
    .agg(
        F.round(F.sum("product_stock_qty") / F.nullif(F.count(F.when(F.col("category") == "Electronics", 1)), F.lit(0)), 2).alias("product_stock_qty_per_electronics"),
        F.min("product_stock_qty").alias("min_product_stock_qty"),
    )
    .filter(F.col("product_stock_qty_per_electronics") >= 0)
)
