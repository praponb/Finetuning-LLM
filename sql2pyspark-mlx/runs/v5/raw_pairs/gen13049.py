from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("price", "id")

result = (
    df_products
    .select(
        "id",
        "category",
        "stock_qty",
        F.lag("price").over(w).alias("prev_price"),
        F.lead("price").over(w).alias("next_price"),
        F.lead("stock_qty").over(w).alias("next_stock_qty"),
        F.row_number().over(w).alias("seq_no"),
    )
)
