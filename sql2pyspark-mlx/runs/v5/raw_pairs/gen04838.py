from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("supplier_id").orderBy(F.col("price").desc())
w4 = Window.partitionBy("category").orderBy("price", "id").rowsBetween(-3, Window.currentRow)

valid = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-02-21").cast("date")) & F.col("supplier_id").isNotNull())
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
        F.upper("product_name").alias("product_name_upper"),
    )
)

result = (
    valid
    .select(
        "id",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "product_name_upper",
        F.sum("price").over(w).alias("running_price"),
        F.sum("stock_qty").over(w2).alias("cumulative_stock_qty"),
        F.cume_dist().over(w3).alias("price_cume_dist"),
        F.avg("cost").over(w4).alias("cost_avg_3"),
    )
)
