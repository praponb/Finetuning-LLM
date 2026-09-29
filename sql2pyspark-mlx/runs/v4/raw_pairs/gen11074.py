from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc())

result = (
    df_products
    .filter(F.col("category") == "Home")
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 3)
    .orderBy(F.col("pos").desc())
)
