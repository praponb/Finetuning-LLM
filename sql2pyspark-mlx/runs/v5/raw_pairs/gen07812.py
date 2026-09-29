from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("product_id").desc_nulls_first())

lines = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select("id", "customer_id", F.col("elem.product_id"), F.col("elem.price"))
)

result = lines.select("id", "price", F.dense_rank().over(w).alias("product_id_dense_rank"))
