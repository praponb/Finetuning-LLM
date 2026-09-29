from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date")

result = (
    df_store_sales
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 2)
    .select("id", "store_id", "sale_date", "product_id")
    .orderBy("store_id", "sale_date")
)
