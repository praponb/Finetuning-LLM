from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())

ranked = (
    df_order_items
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .select("id", "order_id", "quantity", "rank_in_group")
    .orderBy(F.col("quantity").asc_nulls_last(), "order_id")
)
