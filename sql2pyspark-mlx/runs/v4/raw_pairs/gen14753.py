from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity",
        F.round((F.col("unit_price") / F.sum("unit_price").over(w)), 3).alias("unit_price_share"),
        F.ntile(3).over(w2).alias("bucket"),
    )
    .orderBy("unit_price_share")
)
