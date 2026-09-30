from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc())

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-04-01", "2025-04-28"))
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 5)
    .select("id", "store_id", "revenue", "sale_date")
    .orderBy("sale_date", "revenue")
)
