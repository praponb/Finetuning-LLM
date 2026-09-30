from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("cost").desc(), "id")
w_grp = Window.partitionBy("category").orderBy(F.col("stock_qty").desc())
w3 = Window.partitionBy("supplier_id")

result = (
    df_products
    .select(
        "id",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        F.ntile(3).over(w).alias("cost_bucket"),
        F.rank().over(w_grp).alias("stock_qty_rank"),
        F.avg("cost").over(w3).alias("supplier_id_avg_cost"),
    )
)
