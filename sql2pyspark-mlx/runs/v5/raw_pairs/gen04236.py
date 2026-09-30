from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("stock_qty").asc_nulls_first())
w_hist = Window.partitionBy("supplier_id")
w3 = Window.partitionBy("supplier_id", "brand")
w4 = Window.orderBy(F.col("stock_qty").desc())

result = (
    df_products
    .select(
        "id",
        "product_name",
        "cost",
        "launch_date",
        "supplier_id",
        F.cume_dist().over(w).alias("stock_qty_cume_dist"),
        F.corr("stock_qty", "price").over(w_hist).alias("corr_stock_qty_price"),
        F.round((F.col("cost") / F.sum("cost").over(w3)), 4).alias("cost_share"),
        F.cume_dist().over(w4).alias("stock_qty_cume_dist_2"),
    )
)
