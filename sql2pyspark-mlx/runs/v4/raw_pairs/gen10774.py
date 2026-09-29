from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("product_id")

base = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2024-02-28").cast("date"))
    .select("*", F.date_trunc("WEEK", "last_restock").alias("last_restock_period"))
)

result = (
    base
    .withColumn("store_id_min_quantity", F.min("quantity").over(w))
    .withColumn("product_id_avg_quantity", F.avg("quantity").over(w2))
)
