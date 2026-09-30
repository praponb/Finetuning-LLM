from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "supplier_id").orderBy(F.col("stock_qty_per_cost").desc(), "id")

cleaned = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2024-02-05").cast("date"))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.round((F.col("stock_qty") / F.col("cost")), 2).alias("stock_qty_per_cost"),
        F.initcap(F.trim("product_name")).alias("product_name_title"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_filled"),
    )
)

ranked = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.unit_price") < 380), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "stock_qty",
        "supplier_id",
        "product_name_title",
        "brand_filled",
    )
)
