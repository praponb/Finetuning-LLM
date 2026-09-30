from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("line_total").desc_nulls_last())
w2 = Window.partitionBy("product_id").orderBy(F.col("unit_price").asc_nulls_last())

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
    )
)

result = (
    filtered
    .select(
        "*",
        F.cume_dist().over(w).alias("line_total_cume_dist"),
        F.rank().over(w2).alias("unit_price_rank"),
    )
)
