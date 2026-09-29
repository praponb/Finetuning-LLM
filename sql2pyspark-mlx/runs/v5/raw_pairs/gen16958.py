from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price").desc_nulls_last(), "id")

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("quantity_band", F.when(F.col("quantity") >= 8, "High").when(F.col("quantity") >= 4, "Medium").otherwise("Low"))
)

result = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "product_id", "quantity", "unit_price", "quantity_band")
)
