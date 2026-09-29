from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").desc(), "id")

latest = (
    df_order_items
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select("id", "order_id", "product_id", "unit_price", "rank_in_group")
    .orderBy(F.col("id").asc_nulls_last())
    .limit(5)
)
