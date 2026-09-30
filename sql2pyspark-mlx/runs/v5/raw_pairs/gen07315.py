from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price", "id")

result = (
    df_products
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "tags",
        F.lag("cost").over(w).alias("prev_cost"),
        F.lag("stock_qty").over(w).alias("prev_stock_qty"),
        F.row_number().over(w).alias("event_seq"),
    )
)
