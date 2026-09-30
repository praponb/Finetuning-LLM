from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy(F.col("stock_qty").desc_nulls_last())
w2 = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("brand")

result = (
    df_products
    .select(
        "*",
        F.rank().over(w).alias("stock_qty_rank"),
        F.avg("stock_qty").over(w2).alias("stock_qty_avg_2_prior"),
        F.round((F.col("price") / F.sum("price").over(w3)), 4).alias("price_share"),
    )
)
