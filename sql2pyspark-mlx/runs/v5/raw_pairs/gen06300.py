from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("country")
w3 = Window.orderBy("country_avg_product_stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy("country_avg_product_stock_qty", "id")
w5 = Window.partitionBy("city")

valid = (
    df_suppliers
    .filter((~F.col("city").eqNullSafe(F.lit("Tokyo"))) & (F.col("country") == "SG"))
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.lower("country").alias("country_lower"),
    )
)

filtered = (
    valid
    .filter(F.col("city").isNotNull())
    .withColumn("rating_level", F.when(F.col("rating") >= 4.4, "High").when(F.col("rating") >= 2.6, "Medium").otherwise("Low"))
    .withColumn("city_filled", F.coalesce("city", F.lit("N/A")))
)

with_latest = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.stock_qty").alias("product_stock_qty"))
)

scored = (
    with_latest
    .filter(F.col("country") == "UK")
    .withColumn("country_avg_product_stock_qty", F.avg("product_stock_qty").over(w_ordered))
    .withColumn("country_count_product_stock_qty", F.count("product_stock_qty").over(w_ordered))
)

scored_suppliers = (
    scored
    .select(
        "id",
        "country",
        "product_stock_qty",
        "country_avg_product_stock_qty",
        "country_count_product_stock_qty",
        F.sum("rating").over(w3).alias("running_rating"),
        F.lead("rating", 1).over(w4).alias("next_rating"),
        F.round((F.col("rating") / F.sum("rating").over(w5)), 4).alias("rating_share"),
    )
)

result = (
    scored_suppliers
    .withColumn("country_avg_product_stock_qty_tier", F.when(F.col("country_avg_product_stock_qty") >= 379, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["country_avg_product_stock_qty_tier", "country"], ["country_avg_product_stock_qty_tier"], []],
        "country_avg_product_stock_qty_tier",
        "country",
    )
    .agg(
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.percentile_approx("rating_share", 0.9).alias("rating_share_p90"),
        F.round(F.sum("running_rating") / F.nullif(F.count(F.when(F.col("country") == "SG", 1)), F.lit(0)), 2).alias("running_rating_per_sg"),
    )
    .orderBy("country_avg_product_stock_qty_tier")
)
