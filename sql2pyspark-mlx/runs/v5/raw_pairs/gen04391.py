from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc())
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "unit_price",
        "quantity",
        F.rank().over(w).alias("rnk"),
        F.lead("unit_price", 2).over(w2).alias("next_unit_price"),
    )
    .orderBy(F.col("product_id").desc())
)
