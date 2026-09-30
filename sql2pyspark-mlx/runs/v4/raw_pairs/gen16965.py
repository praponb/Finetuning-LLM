from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date")

result = (
    df_products
    .filter(~F.col("product_name").like("%Cof%"))
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 1)
    .orderBy(F.col("stock_qty").desc())
)
