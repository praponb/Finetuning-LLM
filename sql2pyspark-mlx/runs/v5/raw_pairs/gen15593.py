from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "supplier_id")
w_part = Window.partitionBy("category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("supplier_id").orderBy("stock_qty", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("category").orderBy("running_stock_qty", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("category")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

base = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-04-30").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "*",
        F.ceil("price").alias("price_whole"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_filled"),
    )
)

scored = (
    base
    .filter(F.month("launch_date").isin([4, 8, 10]))
    .select(
        "*",
        F.sum("stock_qty").over(w).alias("brand_sum_stock_qty"),
        F.sum("stock_qty").over(w_part).alias("running_stock_qty"),
        F.avg("price_whole").over(w3).alias("price_whole_avg_6"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "category",
        "price",
        "stock_qty",
        "price_whole",
        "brand_sum_stock_qty",
        "running_stock_qty",
        F.min("price").over(w4).alias("last3_min_price"),
        F.max("price").over(w4).alias("last3_max_price"),
        F.sum("price").over(w4).alias("last3_sum_price"),
        F.max("brand_sum_stock_qty").over(w5).alias("category_max_brand_sum_stock_qty"),
    )
)

with_latest = (
    metrics
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.stock_qty",
        "s.brand_sum_stock_qty",
        "s.last3_min_price",
        "s.last3_max_price",
        "s.category_max_brand_sum_stock_qty",
        F.col("c.last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

result = (
    with_latest
    .select(
        "id",
        "category",
        "stock_qty",
        "brand_sum_stock_qty",
        "last3_min_price",
        "last3_max_price",
        "last_restock",
        F.when(F.col("category").rlike("^(Books|Fashion)$"), "GROUP_A").when(F.col("category").rlike("^(Electronics|Home)$"), "PRIORITY").otherwise("CORE").alias("category_zone"),
    )
    .orderBy(F.col("id").desc())
)
