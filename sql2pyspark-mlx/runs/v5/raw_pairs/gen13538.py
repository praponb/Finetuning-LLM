from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("stock_qty").desc(), "id")

result = (
    df_products
    .select("id", "category", "stock_qty", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .orderBy(F.col("id").desc())
)
