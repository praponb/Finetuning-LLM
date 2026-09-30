from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")
w_grp = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc_nulls_first())

result = (
    df_order_items
    .select(
        "id",
        "quantity",
        F.ntile(3).over(w).alias("unit_price_bucket"),
        F.rank().over(w_grp).alias("unit_price_rank"),
    )
)
