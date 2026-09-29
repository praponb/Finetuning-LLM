from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("stock_qty").desc_nulls_last())
w2 = Window.partitionBy("country")
w3 = Window.partitionBy("category").orderBy("rating", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("brand", "category").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country").orderBy("rating", "id")
w6 = Window.partitionBy("cost_tier").orderBy(F.col("row_count").desc())

valid = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.lower(F.trim("product_name")) == "noise-cancel headphones"))
    .withColumn("brand_product_name", F.concat("brand", F.lit(", "), "product_name"))
)

eligible = (
    valid
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.units") != 23), "left_anti")
)

with_dims = (
    eligible
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.country"), F.col("p.supplier_name"))
)

with_dims_products = (
    with_dims
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "left")
    .select(
        "a.*",
        F.col("p.rating"),
        F.col("p.supplier_name").alias("supplier_supplier_name"),
        F.col("p.city"),
    )
)

scored = (
    with_dims_products
    .select(
        "*",
        F.dense_rank().over(w).alias("stock_qty_dense_rank"),
        F.round((F.col("price") / F.sum("price").over(w2)), 4).alias("price_share"),
        F.avg("rating").over(w3).alias("rating_avg_5"),
        F.sum("price").over(w4).alias("cumulative_price"),
        F.ntile(4).over(w5).alias("rating_bucket"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("cost") >= 770, "LARGE").otherwise("SMALL").alias("cost_tier"),
        F.date_trunc("YEAR", "launch_date").alias("year_start"),
    )
    .agg(
        F.min("rating_bucket").alias("min_rating_bucket"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_rows"),
        F.count("*").alias("row_count"),
    )
    .withColumn("row_count_rank", F.rank().over(w6))
    .orderBy(F.col("avg_stock_qty").desc_nulls_last())
)
