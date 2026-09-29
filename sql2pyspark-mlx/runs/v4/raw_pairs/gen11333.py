from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc())

result = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .select(
        "id",
        "order_id",
        "unit_price",
        F.dense_rank().over(w).alias("unit_price_dense_rank"),
    )
)
