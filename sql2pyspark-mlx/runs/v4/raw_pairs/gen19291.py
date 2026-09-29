from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc_nulls_last(), "id")
w2 = Window.partitionBy("brand", "supplier_id").orderBy(F.col("stock_qty").asc_nulls_last(), "id")
w3 = Window.partitionBy("supplier_id", "brand").orderBy(F.col("stock_qty").asc_nulls_last())
w_recent = Window.partitionBy("brand").orderBy("price_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-05-18").cast("date"))
    .select(
        "id",
        "product_name",
        "brand",
        "stock_qty",
        "tags",
        "supplier_id",
        F.floor("price").alias("price_whole"),
        F.coalesce("brand", F.lit("DIRECT")).alias("brand_filled"),
    )
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

ranked_products = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

not_flagged = (
    ranked_products
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.store_id").isin([1, 3, 4, 6])), "left_anti")
)

scored = (
    not_flagged
    .select(
        "*",
        F.percent_rank().over(w3).alias("stock_qty_percent_rank"),
        F.first("stock_qty", ignorenulls=True).over(w_recent).alias("first_known_stock_qty"),
    )
)

result = (
    scored
    .groupBy("supplier_id")
    .pivot("brand", ["Umbrella", "Globex", "Stark"])
    .agg(F.sum("stock_qty_percent_rank"))
)
