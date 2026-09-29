from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("stock_qty").desc_nulls_first())
w2 = Window.partitionBy("supplier_id")
w3 = Window.partitionBy("supplier_id").orderBy("cost", "id")

result = (
    df_products
    .filter(F.col("cost").between(520, 620))
    .select(
        "*",
        F.percent_rank().over(w).alias("stock_qty_percent_rank"),
        F.round((F.col("price") / F.sum("price").over(w2)), 4).alias("price_share"),
        F.row_number().over(w3).alias("row_seq"),
    )
)
