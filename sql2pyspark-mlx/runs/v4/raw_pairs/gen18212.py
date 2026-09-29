from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")
w2 = Window.orderBy(F.col("price").desc_nulls_last())
w3 = Window.orderBy(F.col("stock_qty").desc())
w4 = Window.partitionBy("country")

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .withColumn("brand_group", F.when(F.col("brand").isin(["Stark", "Globex"]), "Core").otherwise("Other"))
    .withColumn("cost_whole", F.ceil("cost"))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("N/A")))
)

remaining_keys = (
    filtered
    .select("category", "id")
    .subtract(
        df_products
        .filter(F.col("product_name").isNotNull())
        .select("category", "id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.category") == F.col("k.category")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.supplier_name"), F.col("p.country"))
)

enriched_products = (
    enriched
    .select(
        "*",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.percent_rank().over(w2).alias("price_percent_rank"),
        F.percent_rank().over(w3).alias("stock_qty_percent_rank"),
        F.avg("stock_qty").over(w4).alias("country_avg_stock_qty"),
    )
)

enriched_products_2 = (
    enriched_products
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.rating"), F.col("b.city"))
)

with_dims = (
    enriched_products_2
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id") == F.col("d.id"), "inner")
    .select(
        "e.*",
        F.col("d.supplier_name").alias("supplier_supplier_name"),
        F.col("d.country").alias("supplier_country"),
    )
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("price_percent_rank") >= 0.61, "LARGE").otherwise("SMALL").alias("price_percent_rank_tier"),
        F.date_trunc("QUARTER", "launch_date").alias("quarter_start"),
    )
    .agg(
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.count(F.when(F.col("supplier_country") == "US", 1)).alias("us_count"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
    .filter(F.col("avg_price") < 0)
    .orderBy(F.col("quarter_start").desc())
)
