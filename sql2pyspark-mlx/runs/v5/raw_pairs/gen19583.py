from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc())

result = (
    df_store_sales
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 3)
    .select("id", "store_id", "sale_date", "revenue")
    .orderBy(F.col("store_id").desc(), "sale_date")
)
