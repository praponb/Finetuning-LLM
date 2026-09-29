from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("supplier_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_products
    .filter(F.col("brand").isNull())
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.substring("product_name", 1, 4).alias("product_name_prefix"),
        F.coalesce("brand", F.lit("DIRECT")).alias("brand_filled"),
    )
)

result = (
    valid
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "supplier_id",
        "product_name_prefix",
        F.last("cost", ignorenulls=True).over(w).alias("last_known_cost"),
        F.sum("stock_qty").over(w2).alias("running_stock_qty"),
    )
)
