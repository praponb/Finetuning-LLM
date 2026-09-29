from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity")

ranked = (
    df_order_items
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .select("id", "product_id", "unit_price", "rank_in_group")
    .orderBy(F.col("product_id").desc())
)
