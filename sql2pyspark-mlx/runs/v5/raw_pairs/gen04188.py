from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity")

result = (
    df_order_items
    .filter(F.col("unit_price") >= 210)
    .select(
        "id",
        "product_id",
        "quantity",
        F.percent_rank().over(w).alias("quantity_pct_rank"),
    )
)
