from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy(F.col("quantity").desc(), "id")
w_grp = Window.partitionBy("order_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .select(
        "*",
        F.ntile(3).over(w_hist).alias("quantity_bucket"),
        F.lag("unit_price", 2).over(w_grp).alias("prev_unit_price"),
    )
)
