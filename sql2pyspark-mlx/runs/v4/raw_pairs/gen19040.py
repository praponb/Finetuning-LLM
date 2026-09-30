from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("price", "id")
w2 = Window.partitionBy("supplier_id")

valid = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2024-03-04").cast("date")) & (F.col("category") == "Fashion"))
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
        F.lower("brand").alias("brand_lower"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_clean"),
    )
)

result = (
    valid
    .select(
        "*",
        F.ntile(3).over(w).alias("price_bucket"),
        F.min("stock_qty").over(w2).alias("supplier_id_min_stock_qty"),
    )
)
