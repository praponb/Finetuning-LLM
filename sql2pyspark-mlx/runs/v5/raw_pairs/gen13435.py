from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("supplier_id", "category").orderBy("launch_date", "id")
w2 = Window.partitionBy("brand")

result = (
    df_products
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        F.lag("cost", 1).over(w_recent).alias("prev_cost"),
        F.round((F.col("price") / F.sum("price").over(w2)), 4).alias("price_share"),
    )
)
