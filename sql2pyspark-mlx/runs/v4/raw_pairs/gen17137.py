from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w_ordered = Window.partitionBy("product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .withColumn("store_id_stddev_samp_quantity", F.stddev_samp("quantity").over(w))
    .withColumn("seq_no", F.row_number().over(w_ordered))
)
