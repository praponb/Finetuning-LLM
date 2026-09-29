from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").desc_nulls_first(), "id")

latest = (
    df_order_items
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .groupBy("product_id")
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.min("unit_price").alias("min_unit_price"),
        F.corr("unit_price", "quantity").alias("corr_unit_price_quantity"),
    )
)
