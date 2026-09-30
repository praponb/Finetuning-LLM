from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id")
w2 = Window.partitionBy("category")
w3 = Window.partitionBy("supplier_id").orderBy("cost", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("supplier_id", "category").orderBy("category_min_stock_qty", "id")

filtered = (
    df_products
    .filter(F.col("launch_date") >= F.to_date(F.lit("2023-04-19")))
    .withColumn("brand_product_name", F.concat("brand", F.lit(", "), "product_name"))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("OTHER")))
)

metrics = (
    filtered
    .select(
        "id",
        "product_name",
        "category",
        "stock_qty",
        "supplier_id",
        "brand_filled",
        F.corr("cost", "price").over(w).alias("corr_cost_price"),
        F.min("stock_qty").over(w2).alias("category_min_stock_qty"),
        F.avg("price").over(w3).alias("price_avg_6"),
    )
)

joined = (
    metrics
    .alias("s")
    .join(df_suppliers.alias("p2"), F.col("s.supplier_id") == F.col("p2.id"), "inner")
    .select("s.*", F.col("p2.city"))
)

filtered_products = (
    joined
    .filter(F.col("city").isNotNull() & F.col("product_name").like("%Bott%"))
    .select(
        "id",
        "product_name",
        "category",
        "supplier_id",
        "corr_cost_price",
        "category_min_stock_qty",
        "price_avg_6",
        F.trim("product_name").alias("clean_product_name"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city"),
    )
)

latest = (
    filtered_products
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "product_name",
        "category",
        "supplier_id",
        "category_min_stock_qty",
        "price_avg_6",
        "clean_product_name",
        "resolved_city",
    )
)

result = (
    latest
    .select("id", "product_name", "category", "price_avg_6", "clean_product_name")
    .orderBy(F.col("id").desc_nulls_last())
)
