from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("units") > 20)
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 3)
    .select("id", "store_id", "sale_date", "product_id")
)
