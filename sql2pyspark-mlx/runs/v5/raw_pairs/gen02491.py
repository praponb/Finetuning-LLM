from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("category")

result = (
    df_products
    .select(
        "id",
        "category",
        "price",
        "cost",
        "stock_qty",
        F.max("cost").over(w_grp).alias("running_max_cost"),
        F.min("cost").over(w_grp).alias("running_min_cost"),
        F.avg("cost").over(w_grp).alias("running_avg_cost"),
        F.corr("stock_qty", "price").over(w_ordered).alias("corr_stock_qty_price"),
    )
)
