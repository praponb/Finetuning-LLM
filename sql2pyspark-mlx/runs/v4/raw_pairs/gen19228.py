from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id")

result = (
    df_order_items
    .filter(F.col("product_id").isin([9, 18]))
    .select(
        "id",
        "quantity",
        "unit_price",
        F.lead("unit_price", 1).over(w).alias("next_unit_price"),
    )
)
