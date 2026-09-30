from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("supplier_id").orderBy("price", "id")

result = (
    df_products
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "stock_qty",
        F.lag("stock_qty").over(w_ordered).alias("prev_stock_qty"),
        F.lead("stock_qty").over(w_ordered).alias("next_stock_qty"),
        F.lag("cost").over(w_ordered).alias("prev_cost"),
        F.lead("cost").over(w_ordered).alias("next_cost"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)
