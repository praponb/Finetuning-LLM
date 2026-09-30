from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("supplier_id").orderBy("stock_qty", "id")
w_hist = Window.partitionBy("brand", "supplier_id").orderBy(F.col("price").desc(), "id")

result = (
    df_products
    .withColumn("stock_qty_bucket", F.ntile(3).over(w_part))
    .withColumn("price_bucket", F.ntile(4).over(w_hist))
)
