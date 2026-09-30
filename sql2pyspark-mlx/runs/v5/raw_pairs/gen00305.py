from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("stock_qty")

result = (
    df_products
    .filter(F.col("brand") == "Globex")
    .select("id", "stock_qty", F.rank().over(w).alias("rnk"))
)
