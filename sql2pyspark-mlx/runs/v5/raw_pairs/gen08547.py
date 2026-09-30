from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_inventory
    .filter(F.month("last_restock").isin([2, 6, 11]))
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 2)
)
