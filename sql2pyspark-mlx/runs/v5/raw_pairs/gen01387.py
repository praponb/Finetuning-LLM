from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")
w2 = Window.partitionBy("supplier_id")

result = (
    df_products
    .filter(F.col("category").isin(["Books", "Fashion"]))
    .select(
        "id",
        "brand",
        "price",
        "supplier_id",
        F.avg("price").over(w).alias("brand_avg_price"),
        F.round((F.col("price") / F.sum("price").over(w2)), 3).alias("price_share"),
    )
)
