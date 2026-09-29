from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc())

result = (
    df_store_sales
    .filter(F.col("sale_date") < "2024-05-12")
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") == 1)
    .select("id", "product_id", "revenue")
)
