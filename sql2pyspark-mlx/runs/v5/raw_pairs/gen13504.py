from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")

result = (
    df_products
    .select("id", "brand", "price", F.min("price").over(w).alias("brand_min_price"))
)
