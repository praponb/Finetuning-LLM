from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")

result = (
    df_inventory
    .withColumn("avg_quantity_by_store", F.avg("quantity").over(w))
    .filter(F.col("quantity") > F.col("avg_quantity_by_store"))
    .select("id", "store_id", "quantity")
    .orderBy(F.col("quantity").desc())
)
