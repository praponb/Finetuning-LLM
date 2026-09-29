from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "stock_qty",
        F.min("price").over(w).alias("running_min_price"),
        F.avg("price").over(w).alias("running_avg_price"),
        F.max("price").over(w).alias("running_max_price"),
        F.count("price").over(w).alias("running_count_price"),
    )
)
