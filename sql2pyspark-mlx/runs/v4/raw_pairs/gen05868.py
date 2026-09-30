from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc(), "id")

result = (
    df_products
    .filter(F.col("brand") == "Globex")
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 5)
    .select("id", "brand", "stock_qty")
    .orderBy(F.col("id").desc())
)
