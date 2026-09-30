from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id")
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id")

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([15, 17]))
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.when(F.col("unit_price") >= 420, "High").when(F.col("unit_price") >= 160, "Medium").otherwise("Low").alias("unit_price_level"),
    )
)

result = (
    valid
    .select(
        "id",
        "product_id",
        "unit_price_level",
        F.row_number().over(w).alias("seq_no"),
        F.ntile(10).over(w2).alias("unit_price_bucket"),
    )
)
