from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc(), "id")

result = (
    df_store_sales
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") == 1)
    .select("id", "product_id", "sale_date")
    .orderBy(F.col("product_id").desc())
)
