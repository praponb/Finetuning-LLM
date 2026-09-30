from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("price").desc(), "id")
w2 = Window.partitionBy("supplier_id", "brand")
w3 = Window.orderBy("launch_date", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("supplier_id").orderBy("stock_qty", "id")

result = (
    df_products
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        F.ntile(4).over(w).alias("price_bucket"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
        F.avg("cost").over(w3).alias("cost_avg_6_prior"),
        F.ntile(3).over(w4).alias("stock_qty_bucket"),
    )
)
