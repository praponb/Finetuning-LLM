from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isin([12, 14, 33, 35]))
    .withColumn("adjusted_unit_price", F.round((F.col("unit_price") * 1.1), 2))
)

result = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
