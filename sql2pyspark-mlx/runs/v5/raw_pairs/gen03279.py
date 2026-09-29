from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("stock_qty").desc(), "id")

result = (
    df_products
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 3)
    .orderBy(F.col("price").desc())
)
