from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("stock_qty", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("supplier_id", "brand")

result = (
    df_products
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "stock_qty",
        "tags",
        F.avg("cost").over(w_grp).alias("cost_avg_2_prior"),
        F.corr("stock_qty", "price").over(w2).alias("corr_stock_qty_price"),
    )
)
