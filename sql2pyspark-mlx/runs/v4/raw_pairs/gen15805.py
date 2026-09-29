from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("quantity")

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "order_id",
        "quantity",
        F.percent_rank().over(w).alias("quantity_pct_rank"),
    )
)
