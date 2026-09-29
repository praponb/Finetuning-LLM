from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")

result = (
    df_inventory
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "product_id", "quantity", "store_id")
)
