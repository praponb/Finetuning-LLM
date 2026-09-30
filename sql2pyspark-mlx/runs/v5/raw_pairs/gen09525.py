from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_last())

scored = (
    df_order_items
    .select(
        "id",
        "order_id",
        "product_id",
        F.avg("unit_price").over(w).alias("product_id_avg_unit_price"),
        F.rank().over(w2).alias("quantity_rank"),
    )
)

result = scored.select("id", "product_id_avg_unit_price", "quantity_rank")
