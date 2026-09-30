from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("product_category", "country").orderBy("product_stock_qty", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy("product_stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_suppliers
    .filter(F.col("rating") >= 3.4)
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.trim("supplier_name").alias("clean_supplier_name"),
        F.concat("supplier_name", F.lit(" - "), "city").alias("supplier_name_city"),
        F.coalesce("city", F.lit("N/A")).alias("city_clean"),
    )
)

latest_match = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.category").alias("product_category"),
        F.col("c.stock_qty").alias("product_stock_qty"),
    )
)

kept = (
    latest_match
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (~F.col("ch.category").isin(["Home", "Sports"])), "left_semi")
)

metrics = (
    kept
    .select(
        "id",
        "country",
        "rating",
        "clean_supplier_name",
        "city_clean",
        "product_category",
        "product_stock_qty",
        F.min("product_stock_qty").over(w2).alias("last3_min_product_stock_qty"),
        F.max("product_stock_qty").over(w2).alias("last3_max_product_stock_qty"),
        F.count("product_stock_qty").over(w2).alias("last3_count_product_stock_qty"),
        F.sum("product_stock_qty").over(w3).alias("running_product_stock_qty"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("last3_max_product_stock_qty") >= 410, "HIGH").otherwise("LOW").alias("last3_max_product_stock_qty_tier"),
    )
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.sort_array(F.collect_set("product_category")).alias("product_category_list"),
        F.max(F.when(F.col("product_category") == "Grocery", F.col("rating"))).alias("grocery_max_rating"),
        F.count(F.when(F.col("product_category") == "Sports", 1)).alias("sports_rows"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("product_category") == "Electronics", 1)), F.lit(0)), 2).alias("rating_per_electronics"),
        F.size(F.collect_set("country")).alias("num_countries_2"),
    )
    .orderBy(F.col("num_countries").desc_nulls_last())
)
