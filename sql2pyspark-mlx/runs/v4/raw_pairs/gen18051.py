from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc())

result = (
    df_store_sales
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 1)
    .select("id", "product_id", "sale_date")
    .orderBy("product_id")
)
