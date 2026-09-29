from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc())

result = (
    df_order_items
    .filter(F.col("quantity").between(4, 5))
    .select(
        "id",
        "product_id",
        "unit_price",
        F.percent_rank().over(w).alias("unit_price_pct_rank"),
    )
    .orderBy(F.col("unit_price").desc())
)
