from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "category").orderBy(F.col("cost").desc(), "id")

deduped = (
    df_products
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select("id", "product_name", "category", "brand", "price", "launch_date", "tags")
    .orderBy(F.col("brand").desc_nulls_first())
)
