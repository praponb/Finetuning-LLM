from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("price_tier").orderBy(F.col("home_stock_qty").desc())

filtered = (
    df_products
    .filter(F.col("brand").isNotNull() & (F.col("launch_date") >= F.lit("2023-09-30").cast("date")))
    .select(
        "*",
        F.concat("category", F.lit(" "), "brand").alias("category_brand"),
        F.initcap(F.trim("product_name")).alias("product_name_title"),
        F.coalesce("brand", F.lit("N/A")).alias("brand_filled"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("price") >= 1130, "LARGE").otherwise("SMALL").alias("price_tier"),
        F.when(F.col("cost") >= 630, "HIGH").otherwise("LOW").alias("cost_tier"),
        "supplier_id",
    )
    .agg(
        F.sum(F.when(F.col("category") == "Home", F.col("stock_qty")).otherwise(0)).alias("home_stock_qty"),
        F.percentile_approx("cost", 0.9).alias("cost_p90"),
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_rows"),
        F.round(F.sum("price") / F.nullif(F.count(F.when(F.col("brand") == "Umbrella", 1)), F.lit(0)), 2).alias("price_per_umbrella"),
        F.stddev_samp("cost").alias("cost_stddev"),
    )
    .withColumn("home_stock_qty_rank", F.rank().over(w))
    .orderBy("fashion_rows")
)
