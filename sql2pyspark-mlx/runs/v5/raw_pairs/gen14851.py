from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "supplier_id").orderBy("launch_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy("price")

result = (
    df_products
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        "stock_qty",
        "tags",
        F.avg("price").over(w).alias("price_avg_2"),
        F.rank().over(w2).alias("price_rank"),
    )
)
