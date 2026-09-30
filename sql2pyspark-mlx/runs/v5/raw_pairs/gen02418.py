from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").desc(), "id")

valid = (
    df_order_items
    .filter(F.col("order_id").isin([6, 15, 33, 39]))
    .withColumn("unit_price_whole", F.ceil("unit_price"))
)

result = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "order_id", "unit_price_whole")
)
