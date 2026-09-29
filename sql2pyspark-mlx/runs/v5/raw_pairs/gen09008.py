from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("sale_date", "id")

latest = (
    df_store_sales
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "revenue")
)

result = (
    latest
    .select("id")
    .orderBy(F.col("id").asc_nulls_first())
)
