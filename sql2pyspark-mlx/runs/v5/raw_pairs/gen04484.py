from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("category").orderBy("launch_date", "id").rowsBetween(-2, -1)
w2 = Window.orderBy(F.col("price").desc(), "id")
w3 = Window.partitionBy("supplier_id", "brand")

with_windows = (
    df_products
    .filter(F.array_contains("tags", "clearance"))
    .select(
        "id",
        "category",
        "tags",
        "supplier_id",
        F.avg("stock_qty").over(w_ordered).alias("stock_qty_avg_2_prior"),
        F.ntile(10).over(w2).alias("price_bucket"),
        F.round((F.col("cost") / F.sum("cost").over(w3)), 4).alias("cost_share"),
    )
)

result = (
    with_windows
    .select("id", "supplier_id", "stock_qty_avg_2_prior", "price_bucket", "cost_share")
)
