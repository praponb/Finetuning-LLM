from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id")
w3 = Window.partitionBy("product_id")

result = (
    df_inventory
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("product_id_count_quantity", F.count("quantity").over(w3))
)
