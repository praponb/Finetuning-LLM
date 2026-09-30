from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "brand").orderBy("price", "id")
w2 = Window.partitionBy("category").orderBy("price", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("category").orderBy(F.col("price").desc_nulls_last(), "id")

with_windows = (
    df_products
    .select(
        "*",
        F.lead("stock_qty", 2).over(w).alias("next_stock_qty"),
        F.avg("price").over(w2).alias("price_avg_3"),
        F.first("price", ignorenulls=True).over(w3).alias("first_known_price"),
    )
)

valid = (
    with_windows
    .filter(F.col("launch_date") <= "2024-04-01")
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        "next_stock_qty",
        F.length("category").alias("category_length"),
        F.lower("product_name").alias("product_name_lower"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("resolved_brand"),
    )
)

deduped = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

cleaned = (
    deduped
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        "next_stock_qty",
        "product_name_lower",
        "resolved_brand",
        F.concat("category", F.lit("/"), "product_name").alias("category_product_name"),
    )
)

result = (
    cleaned
    .groupBy("supplier_id")
    .pivot("category", ["Grocery", "Books"])
    .agg(F.avg("next_stock_qty").alias("total"), F.count(F.lit(1)).alias("n"))
)
