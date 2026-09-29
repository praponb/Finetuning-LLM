from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("price").desc(), "id")

result = (
    df_products
    .filter(F.col("stock_qty") != 225)
    .select("id", "price", F.row_number().over(w).alias("row_num"))
    .orderBy("row_num")
)
