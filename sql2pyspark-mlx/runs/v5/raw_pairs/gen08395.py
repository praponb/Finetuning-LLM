from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("cost", "id")

result = (
    df_products
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 3)
    .select("id", "category", "cost", "brand")
    .orderBy(F.col("cost").desc(), F.col("brand").desc())
)
